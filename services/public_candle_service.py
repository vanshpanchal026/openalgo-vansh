# services/public_candle_service.py
"""
Public Keyless Candle Service for OpenAlgo.

Utilizes Upstox's public, unauthenticated historical candle CDN endpoints
to fetch historical and intraday OHLCV bars without requiring an API key,
login session, or Bearer token.
Integrates watermark-based incremental fetching directly with DuckDB.
"""

import urllib.parse
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
import pandas as pd

from database.historify_db import (
    get_connection,
    resolve_instrument_keys,
    touch_lru,
    upsert_market_data,
)
from utils.logging import get_logger

logger = get_logger(__name__)

# Upstox public historical base URLs
UPSTOX_HIST_BASE = "https://api.upstox.com/v2/historical-candle"


def _format_upstox_interval(interval: str) -> tuple[str, str]:
    """
    Map OpenAlgo interval to (upstox_interval_param, storage_interval).
    Upstox supports: '1minute', '30minute', 'day', 'week', 'month'
    """
    iv = interval.lower()
    if iv in ("1m", "1minute", "1"):
        return "1minute", "1m"
    elif iv in ("30m", "30minute", "30"):
        return "30minute", "30m"
    elif iv in ("d", "1d", "day"):
        return "day", "D"
    elif iv in ("w", "1w", "week"):
        return "week", "W"
    elif iv in ("m", "1m", "month"):
        return "month", "M"
    else:
        # Default fallback to 1minute for other intraday timeframes (aggregated later)
        return "1minute", "1m"


def fetch_public_candles(
    symbol: str,
    exchange: str = "NSE",
    interval: str = "D",
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[bool, list[dict[str, Any]], str | None]:
    """
    Fetches candles from the public unauthenticated CDN endpoint.
    If candles are found, automatically persists them into local DuckDB storage.

    Returns:
        (success, list_of_candle_dicts, error_message)
    """
    try:
        resolved = resolve_instrument_keys(symbol, exchange)
        if not resolved or not resolved.get("upstox_key"):
            return False, [], f"Could not resolve public instrument key for {symbol}:{exchange}"

        upstox_key = resolved["upstox_key"]
        clean_sym = resolved["clean_symbol"]
        effective_exch = resolved["exchange"]

        upstox_iv, store_iv = _format_upstox_interval(interval)

        # Calculate to_date and from_date safely supporting date/datetime or str
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")

        if isinstance(end_date, (datetime, date)):
            to_str = end_date.strftime("%Y-%m-%d")
        elif isinstance(end_date, str) and end_date.strip():
            to_str = end_date.strip()[:10]
        else:
            to_str = today_str

        if isinstance(start_date, (datetime, date)):
            from_str = start_date.strftime("%Y-%m-%d")
        elif isinstance(start_date, str) and start_date.strip():
            from_str = start_date.strip()[:10]
        else:
            # Query DuckDB watermark for incremental fetch
            with get_connection() as conn:
                row = conn.execute("""
                    SELECT MAX(timestamp) FROM market_data
                    WHERE symbol = ? AND exchange = ? AND interval = ?
                """, [clean_sym, effective_exch, store_iv]).fetchone()
                last_ts = row[0] if row and row[0] else None

            if last_ts:
                # Delta fetch starting 1 day before the latest stored candle
                from_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc).date() - timedelta(days=1)
                from_str = from_dt.strftime("%Y-%m-%d")
            else:
                lookback = 30 if store_iv == "1m" else 1095  # 30 days for 1m, 3 years for D
                from_str = (today - timedelta(days=lookback)).strftime("%Y-%m-%d")

        encoded_key = urllib.parse.quote(upstox_key, safe="")
        url = f"{UPSTOX_HIST_BASE}/{encoded_key}/{upstox_iv}/{to_str}/{from_str}"

        with httpx.Client(follow_redirects=True, timeout=15.0) as client:
            resp = client.get(url, headers={"Accept": "application/json"})

            if resp.status_code != 200:
                return False, [], f"Public CDN returned HTTP {resp.status_code}: {resp.text[:120]}"

            body = resp.json()
            raw_candles = body.get("data", {}).get("candles", [])

            # Also fetch today's intraday bars if interval is 1m and looking up current session
            if store_iv == "1m" and to_str == today.strftime("%Y-%m-%d"):
                try:
                    today_url = f"{UPSTOX_HIST_BASE}/intraday/{encoded_key}/{upstox_iv}"
                    today_resp = client.get(today_url, headers={"Accept": "application/json"})
                    if today_resp.status_code == 200:
                        today_candles = today_resp.json().get("data", {}).get("candles", [])
                        if today_candles:
                            raw_candles = today_candles + raw_candles
                except Exception as e:
                    logger.debug(f"[PublicCandleService] Could not backfill current day intraday: {e}")

        if not raw_candles:
            return True, [], None

        # Upstox returns array: [iso_timestamp, open, high, low, close, volume, oi]
        rows = []
        for c in raw_candles:
            if len(c) >= 6:
                iso_time = c[0]
                # Parse ISO timestamp to Unix epoch seconds
                dt = pd.to_datetime(iso_time)
                ts = int(dt.timestamp())
                rows.append({
                    "timestamp": ts,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5]),
                    "oi": int(c[6]) if len(c) > 6 and c[6] is not None else 0,
                })

        # Sort chronologically
        rows.sort(key=lambda r: r["timestamp"])

        # Auto-cache into DuckDB
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop_duplicates(subset=["timestamp"], keep="last")
            upsert_market_data(df, clean_sym, effective_exch, store_iv)
            touch_lru(clean_sym, effective_exch)
            logger.info(
                f"[PublicCandleService] Synced {len(df)} {store_iv} candles into DuckDB for {effective_exch}:{clean_sym}."
            )

        return True, rows, None

    except Exception as e:
        logger.exception(f"[PublicCandleService] Error fetching public candles for {symbol}:{exchange}: {e}")
        return False, [], str(e)
