# blueprints/scanner_gateway.py
"""
Unified Stock Sync Gateway Blueprint.

Provides an authenticated, source-scoped ingestion gateway for external screeners,
IPO discovery, and custom quantitative scanners.

Route: POST /api/v1/custom/watchlist/sync
"""

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request, session
from sqlalchemy.exc import IntegrityError

from database.auth_db import get_username_by_apikey
from database.scanner_meta_db import (
    commit as meta_db_commit,
    count_other_active_sources_for_symbol,
    deactivate_source_provenance,
    get_active_provenance_symbols,
    rollback as meta_db_rollback,
    upsert_provenance,
    upsert_research_meta,
)
from database.token_db import get_symbol_info, get_token
from database.token_db_enhanced import search_symbols
from database.watchlist_db import (
    MAX_ITEMS_PER_LIST,
    Watchlist,
    WatchlistItem,
    create_watchlist,
    db_session,
    is_flagged_list_name,
)

MAX_NAME_LENGTH = 64
from utils.logging import get_logger
from utils.session import is_session_valid

logger = get_logger(__name__)

scanner_gateway_bp = Blueprint("scanner_gateway_bp", __name__)


def _extract_authenticated_user() -> Tuple[Optional[str], Optional[Tuple[Any, int]]]:
    """
    Authenticate request using either:
    1. Header-based API key (X-API-KEY or Authorization: Bearer <key>) for external tools.
    2. Active browser session cookie for internal/UI requests.

    Returns:
        (user_id, error_response)
        If authenticated, user_id is non-None and error_response is None.
        If failed, user_id is None and error_response is a Flask response tuple.
    """
    # 1. Check API Key in headers
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            api_key = auth_header[7:].strip()

    if api_key:
        # Never log or expose raw API key
        user_id = get_username_by_apikey(api_key)
        if not user_id:
            logger.warning("[Scanner Gateway] Request rejected: Invalid API Key")
            return None, (jsonify({"status": "error", "message": "Invalid API key"}), 403)
        return user_id, None

    # 2. Check active browser session
    if is_session_valid():
        # Enforce anti-CSRF protections on state-mutating requests when using ambient session cookies
        if request.method in ("POST", "PUT", "DELETE", "PATCH"):
            csrf_token = request.headers.get("X-CSRFToken") or request.headers.get("X-CSRF-Token")
            requested_with = request.headers.get("X-Requested-With")
            if not (csrf_token or requested_with == "XMLHttpRequest"):
                logger.warning("[Scanner Gateway] Request rejected: Session auth missing anti-CSRF header")
                return None, (
                    jsonify(
                        {
                            "status": "error",
                            "message": "CSRF verification failed: Session requests require X-Requested-With or X-CSRFToken header.",
                        }
                    ),
                    403,
                )
        user_id = session.get("user")
        if user_id:
            return user_id, None

    logger.warning("[Scanner Gateway] Request rejected: No valid API key or session found")
    return None, (
        jsonify(
            {
                "status": "error",
                "message": "Authentication required. Provide X-API-KEY header or active session.",
            }
        ),
        401,
    )


def _match_series_priority(symbol_str: str, base_sym: str) -> int:
    """
    Score symbol match priority (lower = higher priority).
    Prioritizes canonical equity (-EQ) and SME (-SM, -ST) over BE/BZ and debt/rights.
    """
    s = symbol_str.upper()
    if s == base_sym:
        return 1
    if s.endswith("-EQ") or ("-" not in s):
        return 2
    if s.endswith("-SM") or s.endswith("-ST"):
        return 3
    if s.endswith("-BE") or s.endswith("-BZ"):
        return 4
    return 5


INDEX_ALIASES = {
    "NIFTY 50": "NIFTY",
    "NIFTY50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "BANK NIFTY": "BANKNIFTY",
    "BANKNIFTY": "BANKNIFTY",
}


def _resolve_instrument(
    raw_symbol: str, raw_exchange: Optional[str]
) -> Tuple[bool, Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Dynamically resolve symbol against Angel One token database.
    Normalizes vendor delimiters (NSE:, BSE:, .NS, .BO), maps index aliases,
    and prioritizes regular equity and SME series over lower-ranked series.

    Returns:
        (success, canonical_symbol, exchange, token, error_reason)
    """
    sym = (raw_symbol or "").strip().upper()
    exch = (raw_exchange or "").strip().upper() if raw_exchange else None

    if not sym:
        return False, None, None, None, "Symbol is required"

    # Normalize vendor prefixes (e.g. NSE:INFY, BSE:TCS)
    if ":" in sym:
        parts = sym.split(":", 1)
        prefix_exch, pure_sym = parts[0].strip(), parts[1].strip()
        if prefix_exch in ("NSE", "BSE"):
            if not exch:
                exch = prefix_exch
            sym = pure_sym

    # Normalize vendor suffixes (e.g. RELIANCE.NS, TCS.BO)
    if sym.endswith(".NS"):
        sym = sym[:-3].strip()
        if not exch:
            exch = "NSE"
    elif sym.endswith(".BO"):
        sym = sym[:-3].strip()
        if not exch:
            exch = "BSE"

    # Normalize index aliases
    if sym in INDEX_ALIASES:
        sym = INDEX_ALIASES[sym]

    # Supported exchanges
    valid_exchanges = ("NSE", "BSE")

    # Case A: Exchange is explicitly specified
    if exch:
        if exch not in valid_exchanges:
            return False, None, None, None, f"Unsupported exchange '{exch}'. Supported: NSE, BSE"

        # Direct token check (handles exact symbol, BSE scrip code, or series suffix)
        token = get_token(sym, exch)
        if token:
            sym_info = get_symbol_info(sym, exch)
            canonical = sym_info.symbol if sym_info else sym
            return True, canonical, exch, token, None

        # Fallback: Dynamic ranked search in token database
        matches = search_symbols(sym, exch, limit=10)
        if matches:
            candidates = [
                m
                for m in matches
                if (
                    m.get("symbol", "").upper() == sym
                    or m.get("name", "").upper() == sym
                    or m.get("symbol", "").upper().startswith(f"{sym}-")
                )
                and m.get("token")
            ]
            if candidates:
                candidates.sort(key=lambda m: _match_series_priority(m.get("symbol", ""), sym))
                best = candidates[0]
                return True, best["symbol"], exch, best["token"], None

            return (
                False,
                None,
                None,
                None,
                f"Ambiguous match for '{sym}' on {exch}. None matched base ticker exactly.",
            )

        return False, None, None, None, f"Symbol '{sym}' not found on {exch} in broker master"

    # Case B: Exchange is omitted — check NSE first (market primary), then BSE
    for candidate_exch in valid_exchanges:
        token = get_token(sym, candidate_exch)
        if token:
            sym_info = get_symbol_info(sym, candidate_exch)
            canonical = sym_info.symbol if sym_info else sym
            return True, canonical, candidate_exch, token, None

        matches = search_symbols(sym, candidate_exch, limit=10)
        candidates = [
            m
            for m in matches
            if (
                m.get("symbol", "").upper() == sym
                or m.get("name", "").upper() == sym
                or m.get("symbol", "").upper().startswith(f"{sym}-")
            )
            and m.get("token")
        ]
        if candidates:
            candidates.sort(key=lambda m: _match_series_priority(m.get("symbol", ""), sym))
            best = candidates[0]
            return True, best["symbol"], candidate_exch, best["token"], None

    return False, None, None, None, f"Instrument '{sym}' not found on NSE or BSE in broker master"


def on_instruments_synced(
    user_id: str,
    watchlist_name: str,
    source: str,
    added_items: List[Dict[str, Any]],
    removed_items: List[Dict[str, Any]],
) -> None:
    """
    Decoupled event hook executed whenever a sync finishes.
    Provides an extension point for future WhatsApp / Telegram alerts
    and fundamental AI research agents without modifying core gateway logic.
    """
    logger.info(
        "[Scanner Gateway Hook] Sync finished for %s (user: %s, source: %s): "
        "+%d added, -%d removed.",
        watchlist_name,
        user_id,
        source,
        len(added_items),
        len(removed_items),
    )


def sync_watchlist_data(
    user_id: str,
    watchlist_name: str,
    source: str,
    raw_items: List[Dict[str, Any]],
    action: str = "append",
    position_pref: str = "top",
) -> Tuple[bool, Dict[str, Any], int]:
    """
    Core programmatic sync function.
    Can be invoked by HTTP routes or internal Python background services/schedulers.
    """
    # 1. Validate watchlist name
    watchlist_name = (watchlist_name or "").strip()
    if not watchlist_name:
        return False, {"status": "error", "message": "watchlist_name is required"}, 400
    if len(watchlist_name) > MAX_NAME_LENGTH:
        return (
            False,
            {
                "status": "error",
                "message": f"watchlist_name must be {MAX_NAME_LENGTH} characters or fewer",
            },
            400,
        )
    if is_flagged_list_name(watchlist_name):
        return (
            False,
            {"status": "error", "message": "Cannot sync directly to system flagged color lists"},
            400,
        )

    # 2. Validate source
    source = (source or "").strip()
    if not source:
        return False, {"status": "error", "message": "source identifier is required"}, 400

    # 3. Validate action and position
    action = (action or "append").strip().lower()
    if action not in ("append", "replace"):
        return False, {"status": "error", "message": "action must be 'append' or 'replace'"}, 400

    position_pref = (position_pref or "top").strip().lower()
    if position_pref not in ("top", "bottom"):
        return False, {"status": "error", "message": "position must be 'top' or 'bottom'"}, 400

    # 4. Validate items array
    if raw_items is None or not isinstance(raw_items, list):
        return False, {"status": "error", "message": "items must be a list"}, 400
    if len(raw_items) > MAX_ITEMS_PER_LIST:
        return (
            False,
            {
                "status": "error",
                "message": f"Batch size ({len(raw_items)}) exceeds maximum allowed ({MAX_ITEMS_PER_LIST})",
            },
            400,
        )

    # 5. Ensure watchlist exists and belongs to user
    try:
        watchlist = (
            db_session.query(Watchlist)
            .filter_by(user_id=user_id, name=watchlist_name)
            .first()
        )
        if not watchlist:
            create_watchlist(user_id, watchlist_name)
            watchlist = (
                db_session.query(Watchlist)
                .filter_by(user_id=user_id, name=watchlist_name)
                .first()
            )
            if not watchlist:
                return (
                    False,
                    {
                        "status": "error",
                        "message": f"Could not create watchlist '{watchlist_name}' (cap may be reached)",
                    },
                    400,
                )
    except Exception as e:
        logger.exception("Error checking/creating watchlist: %s", e)
        db_session.rollback()
        meta_db_rollback()
        return False, {"status": "error", "message": "Database error checking watchlist"}, 500

    # 6. Dynamic Symbol Resolution & Validation
    valid_resolved: List[Dict[str, Any]] = []
    invalid_items: List[Dict[str, Any]] = []

    for item in raw_items:
        if not isinstance(item, dict):
            invalid_items.append({"input_symbol": str(item), "reason": "Item must be a JSON object"})
            continue

        raw_sym = item.get("symbol")
        raw_exch = item.get("exchange")
        meta = item.get("metadata") or {}

        ok, canonical_sym, resolved_exch, token, err = _resolve_instrument(raw_sym, raw_exch)
        if not ok:
            invalid_items.append(
                {
                    "input_symbol": raw_sym,
                    "exchange": raw_exch,
                    "reason": err or "Resolution failed",
                }
            )
        else:
            valid_resolved.append(
                {
                    "input_symbol": raw_sym,
                    "resolved_symbol": canonical_sym,
                    "exchange": resolved_exch,
                    "token": token,
                    "metadata": meta,
                }
            )

    # 7. Atomic Execution of Watchlist & Provenance Updates
    try:
        # Auto-tag any pre-existing items with source="manual" if they have no active provenance
        for it in watchlist.items:
            cnt = count_other_active_sources_for_symbol(
                user_id, watchlist_name, it.symbol, it.exchange
            )
            if cnt == 0:
                upsert_provenance(
                    user_id, watchlist_name, it.symbol, it.exchange, source="manual", is_active=True
                )

        existing_items_map = {(it.symbol, it.exchange): it for it in watchlist.items}
        removed_items_out: List[Dict[str, Any]] = []
        added_items_out: List[Dict[str, Any]] = []
        already_present_out: List[Dict[str, Any]] = []

        # If action is 'replace', perform source-scoped removal
        if action == "replace":
            old_source_symbols = get_active_provenance_symbols(user_id, watchlist_name, source)
            new_target_symbols = {(v["resolved_symbol"], v["exchange"]) for v in valid_resolved}
            to_remove = old_source_symbols - new_target_symbols

            for sym, exch in to_remove:
                # Check if other sources or manual additions still require this symbol
                other_count = count_other_active_sources_for_symbol(
                    user_id, watchlist_name, sym, exch, exclude_source=source
                )
                if other_count == 0:
                    item_to_delete = existing_items_map.get((sym, exch))
                    if item_to_delete:
                        if item_to_delete in watchlist.items:
                            watchlist.items.remove(item_to_delete)
                        db_session.delete(item_to_delete)
                        removed_items_out.append({"symbol": sym, "exchange": exch})
                        existing_items_map.pop((sym, exch), None)

            # Deactivate provenance records for removed symbols
            deactivate_source_provenance(
                user_id, watchlist_name, source, list(to_remove)
            )

        # Process valid incoming items
        for v in valid_resolved:
            sym = v["resolved_symbol"]
            exch = v["exchange"]
            meta = v["metadata"]

            # Save research metadata into isolated stock_research_meta
            if meta:
                upsert_research_meta(sym, exch, meta)

            if (sym, exch) in existing_items_map:
                # Already in watchlist: update provenance without duplicating
                upsert_provenance(user_id, watchlist_name, sym, exch, source, is_active=True)
                already_present_out.append(
                    {
                        "input_symbol": v["input_symbol"],
                        "resolved_symbol": sym,
                        "exchange": exch,
                        "token": v["token"],
                        "action": "already_present",
                    }
                )
            else:
                # Check cap before adding
                if len(watchlist.items) >= MAX_ITEMS_PER_LIST:
                    invalid_items.append(
                        {
                            "input_symbol": v["input_symbol"],
                            "exchange": exch,
                            "reason": f"Watchlist reached maximum capacity ({MAX_ITEMS_PER_LIST} items)",
                        }
                    )
                    continue

                new_item = WatchlistItem(
                    watchlist_id=watchlist.id,
                    symbol=sym,
                    exchange=exch,
                    position=0,  # Will be re-indexed below
                )
                db_session.add(new_item)
                watchlist.items.append(new_item)
                existing_items_map[(sym, exch)] = new_item
                upsert_provenance(user_id, watchlist_name, sym, exch, source, is_active=True)

                added_items_out.append(
                    {
                        "input_symbol": v["input_symbol"],
                        "resolved_symbol": sym,
                        "exchange": exch,
                        "token": v["token"],
                        "action": "added",
                    }
                )

        # 8. Re-index Display Positions (Ordered sequentially, no collision)
        if position_pref == "top" and added_items_out:
            added_set = {(a["resolved_symbol"], a["exchange"]) for a in added_items_out}
            # Put added items first, followed by previous items
            ordered_items = [
                it for it in watchlist.items if (it.symbol, it.exchange) in added_set
            ] + [
                it for it in watchlist.items if (it.symbol, it.exchange) not in added_set
            ]
        else:
            # Preserve existing order and append added items at the bottom
            added_set = {(a["resolved_symbol"], a["exchange"]) for a in added_items_out}
            ordered_items = [
                it for it in watchlist.items if (it.symbol, it.exchange) not in added_set
            ] + [
                it for it in watchlist.items if (it.symbol, it.exchange) in added_set
            ]

        for pos, it in enumerate(ordered_items):
            it.position = pos

        # Flush and commit all changes atomically
        try:
            db_session.commit()
            meta_db_commit()
        except IntegrityError as ie:
            logger.warning("[Scanner Gateway] Concurrency collision during commit: %s", ie)
            db_session.rollback()
            meta_db_rollback()
            return (
                False,
                {
                    "status": "error",
                    "message": "Concurrent conflict when updating watchlist; please retry",
                },
                409,
            )

        # Update position numbers in valid response payloads
        pos_lookup = {(it.symbol, it.exchange): it.position for it in watchlist.items}
        for item in added_items_out:
            item["position"] = pos_lookup.get((item["resolved_symbol"], item["exchange"]), 0)
        for item in already_present_out:
            item["position"] = pos_lookup.get((item["resolved_symbol"], item["exchange"]), 0)

        # 9. Trigger Background Chart Pre-cache for newly added items
        if added_items_out:
            try:
                from services.watchlist_precache_service import trigger_watchlist_precache

                trigger_watchlist_precache(
                    [
                        {"symbol": a["resolved_symbol"], "exchange": a["exchange"]}
                        for a in added_items_out
                    ],
                    user_id=user_id,
                )
            except Exception as precache_err:
                logger.debug("Could not auto-trigger precache: %s", precache_err)

        # 10. Trigger Future Event Hook (WhatsApp / Agent notes)
        try:
            on_instruments_synced(
                user_id=user_id,
                watchlist_name=watchlist_name,
                source=source,
                added_items=added_items_out,
                removed_items=removed_items_out,
            )
        except Exception as hook_err:
            logger.warning("[Scanner Gateway] Event hook warning: %s", hook_err)

        return (
            True,
            {
                "status": "success",
                "watchlist_id": watchlist.id,
                "watchlist_name": watchlist.name,
                "action": action,
                "source": source,
                "summary": {
                    "total_received": len(raw_items),
                    "valid_added": len(added_items_out),
                    "already_present": len(already_present_out),
                    "source_replaced": len(removed_items_out),
                    "invalid_skipped": len(invalid_items),
                },
                "valid_items": added_items_out + already_present_out,
                "removed_items": removed_items_out,
                "invalid_items": invalid_items,
            },
            200,
        )

    except Exception as e:
        logger.exception("[Scanner Gateway] Transaction failed during sync: %s", e)
        db_session.rollback()
        meta_db_rollback()
        return False, {"status": "error", "message": f"Sync transaction failed: {str(e)}"}, 500


@scanner_gateway_bp.route("/api/v1/custom/watchlist/sync", methods=["POST"])
def sync_watchlist():
    """
    Unified Sync Gateway Endpoint.
    Ingests symbols from external screeners, IPO discovery, or custom tools.
    """
    user_id, auth_error = _extract_authenticated_user()
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    success, data, code = sync_watchlist_data(
        user_id=user_id,
        watchlist_name=payload.get("watchlist_name"),
        source=payload.get("source"),
        raw_items=payload.get("items"),
        action=payload.get("action", "append"),
        position_pref=payload.get("position", "top"),
    )
    return jsonify(data), code


@scanner_gateway_bp.route("/api/v1/custom/ipo/sync", methods=["POST"])
def trigger_ipo_sync():
    """
    On-demand trigger for IPO discovery and sync into:
    - 'IPO 2026 - Mainboard'
    - 'IPO 2026 - SME'
    """
    user_id, auth_error = _extract_authenticated_user()
    if auth_error:
        return auth_error

    payload = request.get_json(silent=True) or {}
    action = payload.get("action", "append")
    year = str(payload.get("year", "2026")).strip()
    if not re.match(r"^20\d{2}$", year):
        return jsonify({"status": "error", "message": "Invalid year format (must be 20XX)"}), 400

    from services.ipo_discovery_service import sync_ipo_watchlists

    result = sync_ipo_watchlists(user_id=user_id, action=action, year=year)
    status_code = 200 if result.get("status") in ("success", "warning", "skipped") else 500
    return jsonify(result), status_code


@scanner_gateway_bp.route("/api/v1/custom/precache/status", methods=["GET"])
def precache_status():
    """Get live status of chart pre-caching worker."""
    user_id, auth_error = _extract_authenticated_user()
    if auth_error:
        return auth_error

    try:
        from services.watchlist_precache_service import get_precache_status

        return jsonify({"status": "success", "data": get_precache_status()}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
