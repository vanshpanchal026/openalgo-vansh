import importlib
import time
from typing import Any

import pandas as pd

from database.auth_db import get_auth_token_broker
from database.token_db import get_token
from services.broker_busy import BrokerBusyError, broker_busy_result
from utils import real_threading
from utils.broker_backpressure import check_queue_wait
from utils.constants import VALID_EXCHANGES
from utils.logging import get_logger

# Initialize logger
logger = get_logger(__name__)

# Rate limiter: max 3 broker history API requests per second, evenly spaced.
#
# Each caller books the next free start time under a lock and then sleeps until
# it arrives, so concurrent callers are spaced one interval apart. The previous
# version read the time of the last call, slept, and wrote it back, all
# unlocked: callers arriving together computed their sleep from the same stale
# value, woke together, and reached the broker as a burst it rejects.
#
# The lock is a real one because the agent's tools reach this from a real OS
# thread. It guards two float operations; the sleep happens after it is released.
_MIN_HISTORY_INTERVAL = 0.35  # 350ms between calls (~3 req/sec, evenly spaced)
_next_history_slot: float = 0.0
_history_slot_lock = real_threading.Lock()


def _enforce_rate_limit(*, background: bool = False):
    """Wait for this request's turn (~3 per second).

    Interactive user chart requests (background=False) get priority lane access
    so users clicking charts never get queued behind background batch downloads.
    """
    global _next_history_slot
    with _history_slot_lock:
        now = time.monotonic()
        if not background:
            # Interactive user requests get priority: bounded by broker spacing (0.35s)
            # without waiting behind a long background queue.
            slot = max(now, min(_next_history_slot, now + _MIN_HISTORY_INTERVAL))
            wait = slot - now
            check_queue_wait(wait, kind="data")
            _next_history_slot = slot + _MIN_HISTORY_INTERVAL
        else:
            # Background precache jobs take regular sequential slots
            slot = max(now, _next_history_slot)
            wait = slot - now
            _next_history_slot = slot + _MIN_HISTORY_INTERVAL
    if wait > 0:
        time.sleep(wait)


def validate_symbol_exchange(symbol: str, exchange: str) -> tuple[bool, str | None]:
    """
    Validate that a symbol exists for the given exchange.

    Args:
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, NFO)

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Validate exchange
    exchange_upper = exchange.upper()
    if exchange_upper not in VALID_EXCHANGES:
        return False, f"Invalid exchange '{exchange}'. Must be one of: {', '.join(VALID_EXCHANGES)}"

    # Validate symbol exists in master contract
    token = get_token(symbol, exchange_upper)
    if token is None:
        # Check alternate exchange for Indian cash equities (NSE <-> BSE)
        if exchange_upper in ("NSE", "BSE"):
            alt = "BSE" if exchange_upper == "NSE" else "NSE"
            if get_token(symbol, alt) is not None:
                return True, None
        return (
            False,
            f"Symbol '{symbol}' not found for exchange '{exchange}'. Please verify the symbol name and ensure master contracts are downloaded.",
        )

    return True, None


def import_broker_module(broker_name: str) -> Any | None:
    """
    Dynamically import the broker-specific data module.

    Args:
        broker_name: Name of the broker

    Returns:
        The imported module or None if import fails
    """
    try:
        module_path = f"broker.{broker_name}.api.data"
        broker_module = importlib.import_module(module_path)
        return broker_module
    except ImportError as error:
        logger.error(f"Error importing broker module '{module_path}': {error}")
        return None


def get_history_with_auth(
    auth_token: str,
    feed_token: str | None,
    broker: str,
    symbol: str,
    exchange: str,
    interval: str,
    start_date: str,
    end_date: str,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get historical data for a symbol using provided auth tokens.

    Args:
        auth_token: Authentication token for the broker API
        feed_token: Feed token for market data (if required by broker)
        broker: Name of the broker
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)
        interval: Time interval (e.g., 1m, 5m, 15m, 1h, 1d)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Validate symbol and exchange before making broker API call
    is_valid, error_msg = validate_symbol_exchange(symbol, exchange)
    if not is_valid:
        return False, {"status": "error", "message": error_msg}, 400

    broker_module = import_broker_module(broker)
    if broker_module is None:
        return False, {"status": "error", "message": "Broker-specific module not found"}, 404

    try:
        # Initialize broker's data handler based on broker's requirements
        if hasattr(broker_module.BrokerData.__init__, "__code__"):
            # Check number of parameters the broker's __init__ accepts
            param_count = broker_module.BrokerData.__init__.__code__.co_argcount
            if param_count > 2:  # More than self and auth_token
                data_handler = broker_module.BrokerData(auth_token, feed_token)
            else:
                data_handler = broker_module.BrokerData(auth_token)
        else:
            # Fallback to just auth token if we can't inspect
            data_handler = broker_module.BrokerData(auth_token)

        # Call the broker's get_history method
        df = data_handler.get_history(symbol, exchange, interval, start_date, end_date)

        if not isinstance(df, pd.DataFrame):
            raise ValueError("Invalid data format returned from broker")

        # Ensure all responses include 'oi' field, set to 0 if not present
        if "oi" not in df.columns:
            df["oi"] = 0

        return True, {"status": "success", "data": df.to_dict(orient="records")}, 200
    except BrokerBusyError as e:
        return broker_busy_result(e, f"History request for {exchange}:{symbol}")
    except Exception as e:
        logger.exception(f"Error in broker_module.get_history: {e}")
        return False, {"status": "error", "message": str(e)}, 500


def get_history_from_db(
    symbol: str, exchange: str, interval: str, start_date: str, end_date: str
) -> tuple[bool, dict[str, Any], int]:
    """
    Get historical data from DuckDB/Historify database.

    Args:
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)
        interval: Time interval (e.g., 1m, 5m, 15m, 1h, D, W, M, Q, Y)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    try:
        from datetime import date, datetime

        from database.historify_db import get_ohlcv

        # Convert dates to timestamps (handle both string and date objects)
        if isinstance(start_date, date):
            start_dt = datetime.combine(start_date, datetime.min.time())
        else:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")

        if isinstance(end_date, date):
            end_dt = datetime.combine(end_date, datetime.min.time())
        else:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Set end_date to end of day
        end_dt = end_dt.replace(hour=23, minute=59, second=59)

        start_timestamp = int(start_dt.timestamp())
        end_timestamp = int(end_dt.timestamp())

        # Get data from DuckDB
        df = get_ohlcv(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

        if df.empty:
            return (
                False,
                {
                    "status": "error",
                    "message": f"No data found for {symbol}:{exchange} interval {interval} in local database. Download data first using Historify.",
                },
                404,
            )

        # Ensure 'oi' column exists
        if "oi" not in df.columns:
            df["oi"] = 0

        # Reorder columns to match API response format
        columns = ["timestamp", "open", "high", "low", "close", "volume", "oi"]
        df = df[columns]

        return True, {"status": "success", "data": df.to_dict(orient="records")}, 200

    except Exception as e:
        logger.exception(f"Error fetching history from DB: {e}")
        return False, {"status": "error", "message": str(e)}, 500


def get_history(
    symbol: str,
    exchange: str,
    interval: str,
    start_date: str,
    end_date: str,
    api_key: str | None = None,
    auth_token: str | None = None,
    feed_token: str | None = None,
    broker: str | None = None,
    source: str = "api",
    background: bool = False,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get historical data for a symbol.
    Supports both API-based authentication and direct internal calls.

    Args:
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)
        interval: Time interval (e.g., 1m, 5m, 15m, 1h, D, W, M, Q, Y)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        api_key: OpenAlgo API key (for API-based calls)
        auth_token: Direct broker authentication token (for internal calls)
        feed_token: Direct broker feed token (for internal calls)
        broker: Direct broker name (for internal calls)
        source: Data source - 'api' (broker, default) or 'db' (DuckDB/Historify).
            Unsupported values return 400 before a provider is called.

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)

        Unsupported source values return a 400 error before either provider is called.
    """
    if not isinstance(source, str) or source not in {"api", "db"}:
        return (
            False,
            {"status": "error", "message": "Source must be either 'api' or 'db'."},
            400,
        )

    # Resolve canonical symbol and adjust exchange if needed
    from database.token_db import get_symbol_info
    sym_info = get_symbol_info(symbol, exchange)
    if sym_info:
        symbol = sym_info.symbol
        if exchange.upper() in ("NSE", "BSE") and sym_info.exchange in ("NSE", "BSE"):
            if get_token(symbol, exchange.upper()) is None and sym_info.exchange != exchange.upper():
                exchange = sym_info.exchange

    # Source: 'db' - Fetch from DuckDB/Historify database
    if source == "db":
        return get_history_from_db(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )

    # Source: 'api' (default) - Fetch from broker API
    # Cache-first check for interactive chart requests:
    if not background:
        db_success, db_resp, _ = get_history_from_db(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
        )
        if db_success and db_resp.get("data"):
            try:
                from database.historify_db import touch_lru

                touch_lru(symbol, exchange)
            except Exception:
                pass
            return True, db_resp, 200

    # Enforce 3 requests/second rate limit for broker history calls
    try:
        if background:
            _enforce_rate_limit(background=True)
        else:
            _enforce_rate_limit()
    except BrokerBusyError as e:
        return broker_busy_result(e, f"History request for {exchange}:{symbol}")

    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, FEED_TOKEN, broker_name = get_auth_token_broker(
            api_key, include_feed_token=True
        )
        if AUTH_TOKEN is None:
            return False, {"status": "error", "message": "Invalid openalgo apikey"}, 403
        success, response_data, status_code = get_history_with_auth(
            AUTH_TOKEN, FEED_TOKEN, broker_name, symbol, exchange, interval, start_date, end_date
        )

    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        success, response_data, status_code = get_history_with_auth(
            auth_token, feed_token, broker, symbol, exchange, interval, start_date, end_date
        )

    # Case 3: Invalid parameters
    else:
        return (
            False,
            {
                "status": "error",
                "message": "Either api_key or both auth_token and broker must be provided",
            },
            400,
        )

    # Dual-exchange fallback for Indian cash equities (NSE <-> BSE) if primary returned no data
    if (not success or not (isinstance(response_data, dict) and response_data.get("data"))) and exchange.upper() in ("NSE", "BSE"):
        alt_exchange = "BSE" if exchange.upper() == "NSE" else "NSE"
        if get_token(symbol, alt_exchange) is not None:
            logger.info(f"Primary exchange {exchange} returned no data for {symbol}. Trying fallback to {alt_exchange}...")
            if api_key and not (auth_token and broker):
                alt_success, alt_response, alt_code = get_history_with_auth(
                    AUTH_TOKEN, FEED_TOKEN, broker_name, symbol, alt_exchange, interval, start_date, end_date
                )
            elif auth_token and broker:
                alt_success, alt_response, alt_code = get_history_with_auth(
                    auth_token, feed_token, broker, symbol, alt_exchange, interval, start_date, end_date
                )
            else:
                alt_success = False

            if alt_success and isinstance(alt_response, dict) and alt_response.get("data"):
                logger.info(f"Fallback to {alt_exchange} for {symbol} succeeded with {len(alt_response['data'])} bars")
                success, response_data, status_code = alt_success, alt_response, alt_code
                exchange = alt_exchange

    # Auto-cache broker data in DuckDB on cache miss
    if not background and success and isinstance(response_data, dict) and response_data.get("data"):
        try:
            from database.historify_db import touch_lru, upsert_market_data

            df_cache = pd.DataFrame(response_data["data"])
            if not df_cache.empty and "timestamp" in df_cache.columns:
                upsert_market_data(df_cache, symbol, exchange, interval)
                touch_lru(symbol, exchange)
        except Exception as e:
            logger.debug(f"Auto-cache error for {symbol}:{exchange}: {e}")

    return success, response_data, status_code
