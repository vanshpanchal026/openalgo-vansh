import importlib
from typing import Any

from database.auth_db import get_auth_token_broker
from database.token_db import get_token
from services.broker_busy import BrokerBusyError, broker_busy_result
from utils.constants import VALID_EXCHANGES
from utils.logging import get_logger

# Initialize logger
logger = get_logger(__name__)


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
        return (
            False,
            f"Symbol '{symbol}' not found for exchange '{exchange}'. Please verify the symbol name and ensure master contracts are downloaded.",
        )

    return True, None


def validate_symbols_bulk(
    symbols: list[dict[str, str]],
) -> tuple[bool, list[dict[str, Any]], str | None]:
    """
    Validate multiple symbols and their exchanges.

    Args:
        symbols: List of dicts with 'symbol' and 'exchange' keys

    Returns:
        Tuple of (all_valid, validated_symbols_with_errors, first_error_message)
    """
    all_valid = True
    validated = []
    first_error = None

    for item in symbols:
        symbol = item.get("symbol", "")
        exchange = item.get("exchange", "")

        if not symbol or not exchange:
            error = "Missing symbol or exchange in request"
            validated.append({**item, "valid": False, "error": error})
            if all_valid:
                first_error = error
                all_valid = False
            continue

        is_valid, error = validate_symbol_exchange(symbol, exchange)
        validated.append({**item, "valid": is_valid, "error": error})

        if not is_valid and all_valid:
            first_error = error
            all_valid = False

    return all_valid, validated, first_error


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


def get_quotes_with_auth(
    auth_token: str, feed_token: str | None, broker: str, symbol: str, exchange: str
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for a symbol using provided auth tokens.

    Args:
        auth_token: Authentication token for the broker API
        feed_token: Feed token for market data (if required by broker)
        broker: Name of the broker
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)

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

        quotes = data_handler.get_quotes(symbol, exchange)

        if quotes is None:
            return False, {"status": "error", "message": "Failed to fetch quotes"}, 500

        return True, {"status": "success", "data": quotes}, 200
    except BrokerBusyError as e:
        return broker_busy_result(e, f"Quote request for {exchange}:{symbol}")
    except Exception as e:
        # Check if this is a permission error
        error_msg = str(e)
        if "permission" in error_msg.lower() or "insufficient" in error_msg.lower():
            # Log at debug level for permission errors (common with personal APIs)
            logger.debug(f"Quote fetch permission denied: {error_msg}")
        else:
            # Log other errors normally
            logger.exception(f"Error in broker_module.get_quotes: {e}")

        return False, {"status": "error", "message": str(e)}, 500


def get_quotes(
    symbol: str,
    exchange: str,
    api_key: str | None = None,
    auth_token: str | None = None,
    feed_token: str | None = None,
    broker: str | None = None,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for a symbol.
    Supports both API-based authentication and direct internal calls.

    Args:
        symbol: Trading symbol
        exchange: Exchange (e.g., NSE, BSE)
        api_key: OpenAlgo API key (for API-based calls)
        auth_token: Direct broker authentication token (for internal calls)
        feed_token: Direct broker feed token (for internal calls)
        broker: Direct broker name (for internal calls)

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, FEED_TOKEN, broker_name = get_auth_token_broker(
            api_key, include_feed_token=True
        )
        if AUTH_TOKEN is None:
            return False, {"status": "error", "message": "Invalid openalgo apikey"}, 403
        return get_quotes_with_auth(AUTH_TOKEN, FEED_TOKEN, broker_name, symbol, exchange)

    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        return get_quotes_with_auth(auth_token, feed_token, broker, symbol, exchange)

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

def get_lakehouse_quotes(symbols: list[dict[str, str]]) -> list[dict[str, Any]]:
    """
    Fetch latest closing quotes and previous close from local DuckDB lakehouse.
    Used as an ultra-fast (<200ms) fallback when broker is offline, session is expired,
    or broker market-data endpoint doesn't support the scrip (e.g. BSE SME).
    """
    if not symbols:
        return []

    try:
        import pandas as pd
        from database.historify_db import get_connection, _historify_db_lock

        sym_names = list({str(s.get("symbol", "")).upper() for s in symbols if s.get("symbol")})
        if not sym_names:
            return []

        with _historify_db_lock:
            with get_connection() as conn:
                sym_list_sql = ", ".join(f"'{s}'" for s in sym_names)
                query = f"""
                    WITH ranked AS (
                        SELECT 
                            symbol, exchange, timestamp, open, high, low, close, volume,
                            ROW_NUMBER() OVER (PARTITION BY symbol, exchange ORDER BY timestamp DESC) as rn
                        FROM market_data
                        WHERE interval = 'D' AND symbol IN ({sym_list_sql})
                    )
                    SELECT 
                        r1.symbol, r1.exchange, r1.open, r1.high, r1.low, r1.close as ltp, r1.volume,
                        COALESCE(r2.close, r1.open) as prev_close
                    FROM ranked r1
                    LEFT JOIN ranked r2 ON r1.symbol = r2.symbol AND r1.exchange = r2.exchange AND r2.rn = 2
                    WHERE r1.rn = 1;
                """
                df = conn.execute(query).df()

        results_map = {}
        for _, row in df.iterrows():
            sym = str(row["symbol"]).upper()
            exch = str(row["exchange"]).upper()
            results_map[f"{exch}:{sym}"] = {
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "ltp": float(row["ltp"]),
                "prev_close": float(row["prev_close"]),
                "volume": int(row["volume"]) if pd.notnull(row["volume"]) else 0,
                "oi": 0,
            }

        out = []
        for s in symbols:
            sym = str(s.get("symbol", "")).upper()
            exch = str(s.get("exchange", "")).upper()
            key = f"{exch}:{sym}"
            data = results_map.get(key)
            if not data:
                # Try cross-exchange lookup (NSE vs BSE)
                alt_exch = "BSE" if exch == "NSE" else "NSE"
                data = results_map.get(f"{alt_exch}:{sym}")

            if data:
                out.append({"symbol": sym, "exchange": exch, "data": data})
            else:
                out.append({"symbol": sym, "exchange": exch, "error": "No lakehouse quote available"})
        return out
    except Exception as e:
        logger.exception(f"Error fetching lakehouse quotes: {e}")
        return []


def get_multiquotes_with_auth(
    auth_token: str, feed_token: str | None, broker: str, symbols: list
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for multiple symbols using provided auth tokens.

    Args:
        auth_token: Authentication token for the broker API
        feed_token: Feed token for market data (if required by broker)
        broker: Name of the broker
        symbols: List of dicts with 'symbol' and 'exchange' keys

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Validate all symbols before making broker API calls
    all_valid, validated_symbols, first_error = validate_symbols_bulk(symbols)

    # Separate valid and invalid symbols
    valid_symbols = [item for item in validated_symbols if item.get("valid", False)]
    invalid_symbols = [item for item in validated_symbols if not item.get("valid", False)]

    # If no valid symbols, return error
    if not valid_symbols:
        return (
            False,
            {
                "status": "error",
                "message": first_error or "No valid symbols provided",
                "invalid_symbols": [
                    {
                        "symbol": s.get("symbol"),
                        "exchange": s.get("exchange"),
                        "error": s.get("error"),
                    }
                    for s in invalid_symbols
                ],
            },
            400,
        )

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

        # Build results list starting with invalid symbols (marked as errors)
        results = []
        for item in invalid_symbols:
            results.append(
                {
                    "symbol": item.get("symbol"),
                    "exchange": item.get("exchange"),
                    "error": item.get("error"),
                }
            )

        # Check if broker supports multiquotes
        if not hasattr(data_handler, "get_multiquotes"):
            # Fallback: fetch quotes one by one for valid symbols only
            logger.debug(
                f"Broker {broker} doesn't support multiquotes, falling back to individual quotes"
            )
            for item in valid_symbols:
                try:
                    quote = data_handler.get_quotes(item["symbol"], item["exchange"])
                    results.append(
                        {"symbol": item["symbol"], "exchange": item["exchange"], "data": quote}
                    )
                except BrokerBusyError:
                    # The symbols after this one would be refused the same way.
                    raise
                except Exception as e:
                    logger.exception(
                        f"Error fetching quote for {item['exchange']}:{item['symbol']}: {e}"
                    )
                    results.append(
                        {"symbol": item["symbol"], "exchange": item["exchange"], "error": str(e)}
                    )

            return True, {"status": "success", "results": results}, 200

        # Use broker's native multiquotes method with only valid symbols
        # Strip validation metadata before passing to broker
        clean_symbols = [{"symbol": s["symbol"], "exchange": s["exchange"]} for s in valid_symbols]
        multiquotes = data_handler.get_multiquotes(clean_symbols)

        if multiquotes is None:
            multiquotes = []

        # Combine broker results with invalid symbol errors
        combined_results = results + (multiquotes if isinstance(multiquotes, list) else [])

        # Backfill any symbols that errored or didn't return data from lakehouse
        missing_indices = [
            i for i, r in enumerate(combined_results) if r.get("error") or not r.get("data")
        ]
        if missing_indices:
            missing_syms = [
                {"symbol": combined_results[i].get("symbol", ""), "exchange": combined_results[i].get("exchange", "")}
                for i in missing_indices
            ]
            lh_quotes = get_lakehouse_quotes(missing_syms)
            lh_map = {f"{q['exchange']}:{q['symbol']}": q for q in lh_quotes if q.get("data")}
            for i in missing_indices:
                k = f"{combined_results[i].get('exchange', '')}:{combined_results[i].get('symbol', '')}"
                if k in lh_map:
                    combined_results[i] = lh_map[k]

        return True, {"status": "success", "results": combined_results}, 200
    except BrokerBusyError as e:
        return broker_busy_result(e, f"Multi-quote request for {len(valid_symbols)} symbols")
    except Exception as e:
        # Check if this is a permission error
        error_msg = str(e)
        if "permission" in error_msg.lower() or "insufficient" in error_msg.lower():
            logger.debug(f"Multiquote fetch permission denied: {error_msg}")
        else:
            logger.exception(f"Error in broker_module.get_multiquotes: {e}")

        # Fallback to local lakehouse quotes on broker failure
        lh_fallback = get_lakehouse_quotes(symbols)
        if any(r.get("data") for r in lh_fallback):
            logger.info("[QuotesService] Broker multiquotes failed; served from local DuckDB lakehouse.")
            return True, {"status": "success", "results": lh_fallback, "source": "lakehouse"}, 200

        return False, {"status": "error", "message": str(e)}, 500


def get_multiquotes(
    symbols: list,
    api_key: str | None = None,
    auth_token: str | None = None,
    feed_token: str | None = None,
    broker: str | None = None,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get real-time quotes for multiple symbols.
    Supports both API-based authentication and direct internal calls.
    Seamlessly falls back to local lakehouse data when broker is offline/unauthenticated.
    """
    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, FEED_TOKEN, broker_name = get_auth_token_broker(
            api_key, include_feed_token=True
        )
        if AUTH_TOKEN is None:
            from database.auth_db import verify_api_key

            user_id = verify_api_key(api_key)
            if user_id:
                # Valid API key, but broker session not logged in today
                lh_results = get_lakehouse_quotes(symbols)
                return True, {"status": "success", "results": lh_results, "source": "lakehouse"}, 200
            return False, {"status": "error", "message": "Invalid openalgo apikey"}, 403
        return get_multiquotes_with_auth(AUTH_TOKEN, FEED_TOKEN, broker_name, symbols)

    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        return get_multiquotes_with_auth(auth_token, feed_token, broker, symbols)

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
