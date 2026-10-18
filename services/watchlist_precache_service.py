# services/watchlist_precache_service.py
"""
Watchlist Pre-cache Service with 90-Day LRU Cache Management.

Automatically triggers background pre-caching of Daily historical candles
for all symbols in an imported or active watchlist.
Eliminates broker rate limits and allows microsecond-speed chart flipping.
"""

import threading
import time
from datetime import datetime, timedelta
from typing import Any

from database.auth_db import get_api_key_for_tradingview, get_first_available_api_key
from database.historify_db import evict_lru_older_than, get_data_range, touch_lru
from database.token_db import get_br_symbol
from services.historify_service import download_data
from utils.logging import get_logger

logger = get_logger(__name__)

# Precache lock and status
_precache_lock = threading.Lock()
_precache_thread: threading.Thread | None = None
_status: dict[str, Any] = {
    "status": "idle",
    "total": 0,
    "completed": 0,
    "current": "",
    "skipped": 0,
    "errors": 0,
    "last_run_at": None,
}


def get_precache_status() -> dict[str, Any]:
    """Get the current progress status of the precache worker."""
    with _precache_lock:
        return dict(_status)


def _precache_worker(items: list[dict], user_id: str | None = None):
    """Background worker thread that fetches and caches daily candles safely."""
    global _status

    # Get API key
    api_key = None
    if user_id:
        try:
            api_key = get_api_key_for_tradingview(user_id)
        except Exception as e:
            logger.warning(f"Could not get API key for user {user_id}: {e}")

    if not api_key:
        api_key = get_first_available_api_key()

    if not api_key:
        logger.error("[Watchlist Precache] No valid API key found. Pre-cache aborted.")
        with _precache_lock:
            _status["status"] = "failed"
            _status["error"] = "No valid API key found"
        return

    total = len(items)
    with _precache_lock:
        _status["status"] = "running"
        _status["total"] = total
        _status["completed"] = 0
        _status["skipped"] = 0
        _status["errors"] = 0
        _status["started_at"] = datetime.now().isoformat()

    logger.info(f"[Watchlist Precache] Started auto-precaching for {total} symbols...")

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    now_epoch = int(datetime.now().timestamp())

    for idx, item in enumerate(items):
        raw_symbol = (item.get("symbol") or "").strip()
        exchange = (item.get("exchange") or "").strip().upper()

        if not raw_symbol or not exchange:
            with _precache_lock:
                _status["completed"] += 1
            continue

        # Resolve canonical symbol and adjust exchange if needed
        symbol = raw_symbol
        from database.token_db import get_symbol_info, get_token
        sym_info = get_symbol_info(raw_symbol, exchange)
        if sym_info:
            symbol = sym_info.symbol
            if exchange in ("NSE", "BSE") and sym_info.exchange in ("NSE", "BSE"):
                if get_token(symbol, exchange) is None and sym_info.exchange != exchange:
                    exchange = sym_info.exchange
        elif exchange == "BSE" and raw_symbol.isdigit():
            from database.token_db import get_symbol
            resolved = get_symbol(raw_symbol, exchange)
            if resolved:
                symbol = resolved

        with _precache_lock:
            _status["current"] = f"{symbol}:{exchange}"

        try:
            # Check if we already have recent daily data (within last 48 hours)
            data_range = get_data_range(symbol, exchange, "D")
            if data_range and data_range.get("last_timestamp"):
                last_ts = data_range["last_timestamp"]
                # 48 hours = 172800 seconds
                if (now_epoch - last_ts) < 172800:
                    # Data is already fresh! Just update LRU timestamp
                    touch_lru(symbol, exchange)
                    with _precache_lock:
                        _status["skipped"] += 1
                        _status["completed"] += 1
                    continue

            # 1. Try public unauthenticated CDN fetch first (Zero broker API calls, zero rate limits)
            from services.public_candle_service import fetch_public_candles

            pub_ok, pub_candles, pub_err = fetch_public_candles(symbol, exchange, "D")
            if pub_ok and pub_candles:
                touch_lru(symbol, exchange)
                with _precache_lock:
                    _status["completed"] += 1
                continue

            # 2. Final Fallback: Broker API if public CDN returned no data
            success, resp, code = download_data(
                symbol=symbol,
                exchange=exchange,
                interval="D",
                start_date=start_date,
                end_date=end_date,
                api_key=api_key,
            )

            if success:
                touch_lru(symbol, exchange)
            else:
                logger.warning(
                    f"[Watchlist Precache] Broker fallback failed for {symbol}:{exchange}: {resp.get('message', code)}"
                )
                with _precache_lock:
                    _status["errors"] += 1

            # Sleep only when hitting broker fallback
            time.sleep(0.35)
        except Exception as e:
            logger.warning(f"[Watchlist Precache] Error precaching {symbol}:{exchange}: {e}")
            with _precache_lock:
                _status["errors"] += 1

    # Run 90-day LRU Eviction after precache finishes
    try:
        evicted = evict_lru_older_than(days=90)
        if evicted > 0:
            logger.info(f"[Watchlist Precache] LRU Cleanup: evicted {evicted} symbols older than 90 days.")
    except Exception as e:
        logger.warning(f"[Watchlist Precache] Error during LRU eviction: {e}")

    with _precache_lock:
        _status["status"] = "idle"
        _status["current"] = ""
        _status["last_run_at"] = datetime.now().isoformat()

    logger.info(
        f"[Watchlist Precache] Finished: {_status['completed']}/{total} processed "
        f"({_status['skipped']} already cached, {_status['errors']} errors)."
    )


def trigger_watchlist_precache(items: list[dict], user_id: str | None = None) -> bool:
    """
    Auto-trigger background precache worker for a list of symbols.
    Returns True if started, False if already running or empty list.
    """
    global _precache_thread

    if not items:
        return False

    with _precache_lock:
        if _precache_thread is not None and _precache_thread.is_alive():
            logger.info("[Watchlist Precache] Worker is already running. Skipping duplicate trigger.")
            return False

        if len(items) >= 50:
            try:
                from services.bulk_data_seeder import trigger_bulk_seed_async
                trigger_bulk_seed_async()
            except Exception as e:
                logger.debug(f"[Watchlist Precache] Bulk seed trigger skipped: {e}")

        _precache_thread = threading.Thread(
            target=_precache_worker,
            args=(items, user_id),
            name="WatchlistPrecacheWorker",
            daemon=True,
        )
        _precache_thread.start()
        return True
