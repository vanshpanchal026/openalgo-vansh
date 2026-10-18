# test/test_scanner_gateway.py
"""
Unit and integration tests for Unified Scanner Gateway & Provenance Engine.

Verifies:
1. Authentication (API Key, Session, Invalid Key, Unauthenticated)
2. User/Watchlist ownership enforcement (Never trusting payload user_id)
3. Dynamic symbol resolution (NSE, SME, BSE) without hardcoded suffixes
4. Duplicate submissions and idempotency
5. Source-scoped replacement preserving manual entries
6. Multiple sources sharing a symbol (replacing one preserves the other)
7. Rollback when a database operation fails
8. Invalid symbol reporting with diagnostic reasons
9. Research metadata isolation and upserts
"""

import json
import pytest
from flask import Flask

from database import watchlist_db as wl
from database import scanner_meta_db as meta_db
from database.symbol import init_db as init_symbol_db, SymToken
from database.token_db_enhanced import get_cache
from blueprints.scanner_gateway import scanner_gateway_bp

TEST_USER = "gateway_test_user"
OTHER_USER = "gateway_other_user"
VALID_API_KEY = "test_valid_api_key_123"


def _seed_test_symbols():
    """Populate symtoken table with standard test instruments and refresh in-memory cache."""
    test_instruments = [
        {"symbol": "RELIANCE", "brsymbol": "RELIANCE-EQ", "name": "RELIANCE", "exchange": "NSE", "brexchange": "NSE", "token": "2885"},
        {"symbol": "VNL", "brsymbol": "VNL-BE", "name": "VNL", "exchange": "NSE", "brexchange": "NSE", "token": "766474"},
        {"symbol": "RKFAL-ST", "brsymbol": "RKFAL-ST", "name": "RKFAL", "exchange": "NSE", "brexchange": "NSE", "token": "766540"},
        {"symbol": "PARASYNTEX", "brsymbol": "PARASYNTEX", "name": "PARASYNTEX", "exchange": "BSE", "brexchange": "BSE", "token": "544982"},
        {"symbol": "INFY", "brsymbol": "INFY-EQ", "name": "INFY", "exchange": "NSE", "brexchange": "NSE", "token": "1594"},
        {"symbol": "TCS", "brsymbol": "TCS-EQ", "name": "TCS", "exchange": "NSE", "brexchange": "NSE", "token": "11536"},
        {"symbol": "WIPRO", "brsymbol": "WIPRO-EQ", "name": "WIPRO", "exchange": "NSE", "brexchange": "NSE", "token": "3787"},
        {"symbol": "TECHM", "brsymbol": "TECHM-EQ", "name": "TECHM", "exchange": "NSE", "brexchange": "NSE", "token": "13538"},
        {"symbol": "HCLTECH", "brsymbol": "HCLTECH-EQ", "name": "HCLTECH", "exchange": "NSE", "brexchange": "NSE", "token": "7229"},
        {"symbol": "SBIN", "brsymbol": "SBIN-EQ", "name": "SBIN", "exchange": "NSE", "brexchange": "NSE", "token": "3045"},
    ]

    for item in test_instruments:
        existing = SymToken.query.filter_by(symbol=item["symbol"], exchange=item["exchange"]).first()
        if not existing:
            row = SymToken(
                symbol=item["symbol"],
                brsymbol=item["brsymbol"],
                name=item["name"],
                exchange=item["exchange"],
                brexchange=item["brexchange"],
                token=item["token"],
                instrumenttype="",
                tick_size=0.05,
                lotsize=1,
            )
            meta_db.db_session.add(row)
    meta_db.db_session.commit()

    # Refresh the in-memory token cache snapshot
    cache = get_cache()
    cache.load_all_symbols("angel")


@pytest.fixture(autouse=True)
def clean_slate(monkeypatch):
    """Ensure database tables exist, seed test symbols, and clean up test records."""
    init_symbol_db()
    wl.init_db()
    meta_db.init_db()

    _seed_test_symbols()

    # Mock precache service to prevent network calls and sleeps during tests
    monkeypatch.setattr(
        "services.watchlist_precache_service.trigger_watchlist_precache",
        lambda *args, **kwargs: True,
    )

    # Rollback any lingering transaction and clean state for test users
    wl.db_session.rollback()
    meta_db.db_session.rollback()

    for user in (TEST_USER, OTHER_USER):
        for w in wl.get_watchlists(user):
            if not wl.is_flagged_list_name(w["name"]):
                wl.delete_watchlist(user, w["id"])
        meta_db.db_session.query(meta_db.ScannerStockProvenance).filter_by(user_id=user).delete()
    wl.db_session.commit()
    meta_db.db_session.commit()

    yield

    wl.db_session.rollback()
    meta_db.db_session.rollback()
    for user in (TEST_USER, OTHER_USER):
        for w in wl.get_watchlists(user):
            if not wl.is_flagged_list_name(w["name"]):
                wl.delete_watchlist(user, w["id"])
        meta_db.db_session.query(meta_db.ScannerStockProvenance).filter_by(user_id=user).delete()
    wl.db_session.commit()
    meta_db.db_session.commit()

    wl.db_session.remove()
    meta_db.db_session.remove()


@pytest.fixture
def app():
    """Create test Flask application with scanner gateway blueprint."""
    test_app = Flask(__name__)
    test_app.secret_key = "test-secret-key-openalgo"
    test_app.config["TESTING"] = True
    test_app.register_blueprint(scanner_gateway_bp)

    @test_app.teardown_appcontext
    def shutdown_sessions(exception=None):
        import database.watchlist_db as wl
        wl.db_session.remove()

    return test_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_auth_rejection_no_credentials(client):
    """Requests with neither API key nor session must be rejected with 401."""
    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        json={"watchlist_name": "Test WL", "source": "test_src", "items": []},
    )
    assert resp.status_code == 401
    data = resp.get_json()
    assert data["status"] == "error"
    assert "Authentication required" in data["message"]


def test_auth_rejection_invalid_api_key(client, monkeypatch):
    """Requests with an invalid API key must be rejected with 403."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: None)

    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": "fake_invalid_key"},
        json={"watchlist_name": "Test WL", "source": "test_src", "items": []},
    )
    assert resp.status_code == 403
    data = resp.get_json()
    assert data["status"] == "error"
    assert "Invalid API key" in data["message"]


def test_auth_valid_api_key_scopes_to_user(client, monkeypatch):
    """Valid API key must resolve the user and ignore any spoofed user_id in body."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(
        gw,
        "get_username_by_apikey",
        lambda key: TEST_USER if key == VALID_API_KEY else None,
    )

    payload = {
        "user_id": OTHER_USER,
        "watchlist_name": "API Key Watchlist",
        "source": "screener_a",
        "items": [{"symbol": "RELIANCE", "exchange": "NSE"}],
    }

    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "success"

    # Verify watchlist was created for TEST_USER, NOT OTHER_USER
    test_user_lists = [w["name"] for w in wl.get_watchlists(TEST_USER)]
    other_user_lists = [w["name"] for w in wl.get_watchlists(OTHER_USER)]
    assert "API Key Watchlist" in test_user_lists
    assert "API Key Watchlist" not in other_user_lists


def test_dynamic_symbol_resolution(client, monkeypatch):
    """Verify resolution of standard (NSE), SME (NSE), and BSE symbols without hardcoding suffixes."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    payload = {
        "watchlist_name": "Resolution Test",
        "source": "test_resolver",
        "items": [
            {"symbol": "VNL", "exchange": "NSE"},        # Mainboard
            {"symbol": "RKFAL", "exchange": "NSE"},      # SME base -> resolves to RKFAL-ST
            {"symbol": "PARASYNTEX", "exchange": "BSE"}, # BSE equity
        ],
    }

    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["summary"]["valid_added"] == 3
    assert data["summary"]["invalid_skipped"] == 0

    resolved_symbols = [item["resolved_symbol"] for item in data["valid_items"]]
    assert "VNL" in resolved_symbols
    assert "RKFAL-ST" in resolved_symbols
    assert "PARASYNTEX" in resolved_symbols


def test_invalid_symbol_reporting(client, monkeypatch):
    """Unresolvable or malformed symbols must be skipped and reported with reasons."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    payload = {
        "watchlist_name": "Invalid Test",
        "source": "test_invalid",
        "items": [
            {"symbol": "RELIANCE", "exchange": "NSE"},          # Valid
            {"symbol": "XYZ_DOESNOTEXIST", "exchange": "NSE"},  # Unlisted / Invalid
            {"symbol": "INVALID_EXCH", "exchange": "NASDAQ"},   # Unsupported exchange
        ],
    }

    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["summary"]["valid_added"] == 1
    assert data["summary"]["invalid_skipped"] == 2

    invalid_reasons = {item["input_symbol"]: item["reason"] for item in data["invalid_items"]}
    assert "XYZ_DOESNOTEXIST" in invalid_reasons
    assert "INVALID_EXCH" in invalid_reasons
    assert "Unsupported exchange" in invalid_reasons["INVALID_EXCH"]


def test_source_scoped_replace_preserves_manual_and_other_sources(client, monkeypatch):
    """
    Core Contract: 'replace' action must ONLY delete entries belonging to that source.
    Manual stocks and stocks from another source must be preserved!
    """
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    # 1. Create a watchlist and manually add a stock (no provenance / manual)
    created = wl.create_watchlist(TEST_USER, "Hybrid Watchlist")
    wl.add_item(TEST_USER, created["id"], "TCS", "NSE")

    # 2. Add stocks via source 'screener_a'
    resp1 = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={
            "watchlist_name": "Hybrid Watchlist",
            "source": "screener_a",
            "action": "append",
            "items": [{"symbol": "INFY", "exchange": "NSE"}],
        },
    )
    assert resp1.status_code == 200

    # 3. Add stocks via source 'screener_b'
    resp2 = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={
            "watchlist_name": "Hybrid Watchlist",
            "source": "screener_b",
            "action": "append",
            "items": [
                {"symbol": "WIPRO", "exchange": "NSE"},
                {"symbol": "HCLTECH", "exchange": "NSE"},
            ],
        },
    )
    assert resp2.status_code == 200

    # Current Watchlist should have: TCS (manual), INFY (screener_a), WIPRO (screener_b), HCLTECH (screener_b)
    current_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "Hybrid Watchlist"][0]
    syms_before = {it["symbol"] for it in current_wl["items"]}
    assert syms_before == {"TCS", "INFY", "WIPRO", "HCLTECH"}

    # 4. Now run 'replace' for 'screener_b', dropping HCLTECH and keeping WIPRO + adding TECHM
    resp3 = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={
            "watchlist_name": "Hybrid Watchlist",
            "source": "screener_b",
            "action": "replace",
            "items": [
                {"symbol": "WIPRO", "exchange": "NSE"},
                {"symbol": "TECHM", "exchange": "NSE"},
            ],
        },
    )
    assert resp3.status_code == 200

    # Verify after replace:
    # - TCS (manual) MUST STILL EXIST!
    # - INFY (screener_a) MUST STILL EXIST!
    # - WIPRO (re-confirmed by screener_b) MUST STILL EXIST!
    # - TECHM (newly added by screener_b) MUST BE ADDED!
    # - HCLTECH (dropped by screener_b) MUST BE REMOVED!
    updated_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "Hybrid Watchlist"][0]
    syms_after = {it["symbol"] for it in updated_wl["items"]}
    assert "TCS" in syms_after
    assert "INFY" in syms_after
    assert "WIPRO" in syms_after
    assert "TECHM" in syms_after
    assert "HCLTECH" not in syms_after


def test_shared_symbol_across_sources_preserved(client, monkeypatch):
    """
    If a symbol is in BOTH Source A and Source B:
    Replacing Source A (without that symbol) must NOT remove it from the watchlist,
    because Source B still actively manages it.
    """
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    # 1. Source A adds RELIANCE and SBIN
    client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={
            "watchlist_name": "Shared Test",
            "source": "source_a",
            "action": "append",
            "items": [
                {"symbol": "RELIANCE", "exchange": "NSE"},
                {"symbol": "SBIN", "exchange": "NSE"},
            ],
        },
    )

    # 2. Source B ALSO adds RELIANCE
    client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={
            "watchlist_name": "Shared Test",
            "source": "source_b",
            "action": "append",
            "items": [{"symbol": "RELIANCE", "exchange": "NSE"}],
        },
    )

    # Both provenance records must exist
    active_a = meta_db.get_active_provenance_symbols(TEST_USER, "Shared Test", "source_a")
    active_b = meta_db.get_active_provenance_symbols(TEST_USER, "Shared Test", "source_b")
    assert ("RELIANCE", "NSE") in active_a
    assert ("RELIANCE", "NSE") in active_b

    # 3. Source A replaces its list with ONLY SBIN (dropping RELIANCE)
    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={
            "watchlist_name": "Shared Test",
            "source": "source_a",
            "action": "replace",
            "items": [{"symbol": "SBIN", "exchange": "NSE"}],
        },
    )
    assert resp.status_code == 200

    # RELIANCE MUST STILL BE IN THE WATCHLIST because source_b is still active!
    shared_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "Shared Test"][0]
    items = [it["symbol"] for it in shared_wl["items"]]
    assert "RELIANCE" in items
    assert "SBIN" in items

    # Provenance for Source A on RELIANCE must be inactive
    active_a_after = meta_db.get_active_provenance_symbols(TEST_USER, "Shared Test", "source_a")
    assert ("RELIANCE", "NSE") not in active_a_after
    # But Source B provenance is still active
    active_b_after = meta_db.get_active_provenance_symbols(TEST_USER, "Shared Test", "source_b")
    assert ("RELIANCE", "NSE") in active_b_after


def test_research_metadata_storage_decoupled(client, monkeypatch):
    """Research metadata must be stored in stock_research_meta and NOT pollute core watchlist tables."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    payload = {
        "watchlist_name": "Research Test",
        "source": "ipo_discovery",
        "items": [
            {
                "symbol": "VNL",
                "exchange": "NSE",
                "metadata": {
                    "company_name": "Vishal Nirmiti Limited",
                    "listing_date": "2026-10-08",
                    "issue_price": 125.0,
                    "is_sme": False,
                    "subscription_stats": {"qib": 12.5, "retail": 8.4},
                    "agent_notes": {"moat": "High railway concrete sleeper market share"},
                },
            }
        ],
    }

    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp.status_code == 200

    # Query research meta directly
    meta = meta_db.get_research_meta("VNL", "NSE")
    assert meta is not None
    assert meta["company_name"] == "Vishal Nirmiti Limited"
    assert meta["issue_price"] == 125.0
    assert meta["is_sme"] is False
    assert meta["subscription_stats"]["qib"] == 12.5
    assert meta["agent_notes"]["moat"] == "High railway concrete sleeper market share"

    # Core watchlist item has ONLY symbol, exchange, position
    research_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "Research Test"][0]
    wl_items = research_wl["items"]
    assert len(wl_items) == 1
    item = wl_items[0]
    assert item["symbol"] == "VNL"
    assert "company_name" not in item  # Core table is clean


def test_idempotent_repeated_sync(client, monkeypatch):
    """Submitting the exact same sync payload twice must be idempotent."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    payload = {
        "watchlist_name": "Idempotent Test",
        "source": "daily_scanner",
        "items": [{"symbol": "INFY", "exchange": "NSE"}],
    }

    resp1 = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp1.get_json()["summary"]["valid_added"] == 1

    resp2 = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp2.get_json()["summary"]["valid_added"] == 0
    assert resp2.get_json()["summary"]["already_present"] == 1

    # Total items in watchlist must remain 1
    idempotent_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "Idempotent Test"][0]
    assert len(idempotent_wl["items"]) == 1


def test_atomic_rollback_on_failure(client, monkeypatch):
    """If a database error occurs mid-sync, all watchlist and provenance mutations must roll back."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    # Pre-create the watchlist so watchlist existence check passes cleanly
    wl.create_watchlist(TEST_USER, "Rollback Test")

    # Force an unhandled database error during the final sync commit
    def _exploding_commit():
        raise RuntimeError("Simulated Database Disk I/O Failure")

    monkeypatch.setattr(gw.db_session, "commit", _exploding_commit)

    payload = {
        "watchlist_name": "Rollback Test",
        "source": "exploding_source",
        "items": [{"symbol": "RELIANCE", "exchange": "NSE"}],
    }

    resp = client.post(
        "/api/v1/custom/watchlist/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json=payload,
    )
    assert resp.status_code == 500
    assert "Sync transaction failed" in resp.get_json()["message"]

    # Restore commit before verifying database state and running fixture teardown
    monkeypatch.undo()

    # Verify no provenance was committed for 'exploding_source'
    provs = meta_db.get_active_provenance_symbols(TEST_USER, "Rollback Test", "exploding_source")
    assert len(provs) == 0

    # Verify RELIANCE was rolled back and not added to the watchlist
    rollback_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "Rollback Test"][0]
    assert len(rollback_wl["items"]) == 0
