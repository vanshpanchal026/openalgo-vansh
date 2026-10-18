# services/live_candle_aggregator.py
"""
Live In-Memory Candle Aggregator for Market Hours (09:15 - 15:35 IST).

Merges real-time LTP ticks from Angel One WebSocket into forming 1m, 5m, 15m, and Daily
candlesticks in memory. Enables real-time chart updates without hitting broker REST historical APIs.
"""

import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from utils.logging import get_logger

logger = get_logger(__name__)

# IST timezone: UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))


def _get_ist_now() -> datetime:
    """Return current datetime in IST."""
    return datetime.now(timezone.utc).astimezone(IST)


def _is_market_hours() -> bool:
    """
    Check if current time is within Indian market trading hours (09:00 - 15:40 IST Mon-Fri).
    """
    now = _get_ist_now()
    if now.weekday() >= 5:  # Saturday or Sunday
        return False
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=40, second=0, microsecond=0)
    return market_open <= now <= market_close


class LiveCandleAggregator:
    """
    In-memory aggregator for live forming candlesticks.
    Maintains the current minute and daily candles per symbol:exchange.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # Key: (symbol, exchange) -> dict of interval forming bars
        # Example: { ("RELIANCE", "NSE"): { "1m": {...}, "D": {...} } }
        self._forming_candles: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}

    def on_tick(
        self,
        symbol: str,
        exchange: str,
        ltp: float,
        volume: int = 0,
        epoch_sec: int | None = None,
    ) -> None:
        """
        Ingest a live tick and update the forming candles.
        
        Args:
            symbol: Trading symbol (e.g. RELIANCE, NIFTY)
            exchange: Exchange (e.g. NSE, BSE)
            ltp: Last Traded Price
            volume: Cumulative or tick volume
            epoch_sec: Tick epoch timestamp in seconds (defaults to current time)
        """
        if ltp <= 0:
            return

        now_sec = epoch_sec if epoch_sec and epoch_sec > 0 else int(time.time())
        sym_key = (symbol.upper(), exchange.upper())

        # 1-minute bucket (floored to 60 seconds)
        minute_ts = (now_sec // 60) * 60

        # Daily bucket (floored to IST midnight or day start)
        tick_dt_ist = datetime.fromtimestamp(now_sec, tz=timezone.utc).astimezone(IST)
        day_start_ist = tick_dt_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        day_ts = int(day_start_ist.timestamp())

        with self._lock:
            if sym_key not in self._forming_candles:
                self._forming_candles[sym_key] = {}

            sym_bars = self._forming_candles[sym_key]

            # 1. Update 1m forming candle
            current_1m = sym_bars.get("1m")
            if not current_1m or current_1m["timestamp"] != minute_ts:
                # New minute started
                sym_bars["1m"] = {
                    "timestamp": minute_ts,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": volume,
                    "oi": 0,
                }
            else:
                current_1m["high"] = max(current_1m["high"], ltp)
                current_1m["low"] = min(current_1m["low"], ltp)
                current_1m["close"] = ltp
                if volume > 0:
                    current_1m["volume"] = max(current_1m["volume"], volume)

            # 2. Update Daily forming candle
            current_d = sym_bars.get("D")
            if not current_d or current_d["timestamp"] != day_ts:
                # New day started
                sym_bars["D"] = {
                    "timestamp": day_ts,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": volume,
                    "oi": 0,
                }
            else:
                current_d["high"] = max(current_d["high"], ltp)
                current_d["low"] = min(current_d["low"], ltp)
                current_d["close"] = ltp
                if volume > 0:
                    current_d["volume"] = max(current_d["volume"], volume)

    def get_forming_candle(
        self, symbol: str, exchange: str, interval: str
    ) -> dict[str, Any] | None:
        """
        Get the current active forming candle for a given symbol and interval.
        Returns None if no live ticks have been recorded for this symbol.
        """
        sym_key = (symbol.upper(), exchange.upper())
        with self._lock:
            sym_bars = self._forming_candles.get(sym_key)
            if not sym_bars:
                return None

            if interval in ("D", "1D", "d"):
                bar = sym_bars.get("D")
                return dict(bar) if bar else None

            if interval in ("1m", "1M", "1"):
                bar = sym_bars.get("1m")
                return dict(bar) if bar else None

            # For 5m, 15m, 1h: resample from 1m forming bar aligned to market open (09:15 IST)
            bar_1m = sym_bars.get("1m")
            if not bar_1m:
                return None

            now_sec = bar_1m["timestamp"]
            mult_map = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}
            mult = mult_map.get(interval.lower())
            if not mult:
                return None

            # Calculate bucket floor aligned to 09:15 IST (33300 sec from IST midnight)
            # 9 hours * 3600 + 15 min * 60 = 33300 seconds
            market_open_offset = 33300
            tick_dt_ist = datetime.fromtimestamp(now_sec, tz=timezone.utc).astimezone(IST)
            day_start_ist = tick_dt_ist.replace(hour=0, minute=0, second=0, microsecond=0)
            day_start_epoch = int(day_start_ist.timestamp())

            sec_since_day = now_sec - day_start_epoch
            sec_bucket = mult * 60

            if sec_since_day >= market_open_offset:
                bucket_index = (sec_since_day - market_open_offset) // sec_bucket
                bucket_start_epoch = day_start_epoch + market_open_offset + (bucket_index * sec_bucket)
            else:
                bucket_start_epoch = day_start_epoch + ((sec_since_day // sec_bucket) * sec_bucket)

            return {
                "timestamp": bucket_start_epoch,
                "open": bar_1m["open"],
                "high": bar_1m["high"],
                "low": bar_1m["low"],
                "close": bar_1m["close"],
                "volume": bar_1m["volume"],
                "oi": 0,
            }

    def clear(self) -> None:
        """Clear all forming candle state."""
        with self._lock:
            self._forming_candles.clear()


# Global Singleton Instance
live_aggregator = LiveCandleAggregator()
