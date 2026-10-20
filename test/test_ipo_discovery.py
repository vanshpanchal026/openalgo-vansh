# test/test_ipo_discovery.py
"""
Unit and Integration Tests for Automated IPO Discovery, Dual Watchlists,
Scheduler, and Pre-cache Integration (Phase B).
"""

import json
import pytest
from flask import Flask

import database.scanner_meta_db as meta_db
import database.watchlist_db as wl
from blueprints.scanner_gateway import scanner_gateway_bp
from database.symbol import SymToken, init_db as init_symbol_db
from database.token_db_enhanced import get_cache
from services.ipo_discovery_service import (
    fetch_trendlyne_ipos,
    init_ipo_scheduler,
    shutdown_ipo_scheduler,
    sync_ipo_watchlists,
)

TEST_USER = "ipo_test_user"
VALID_API_KEY = "openalgo_valid_test_api_key_ipo"


def _seed_test_symbols():
    """Ensure core symbols exist in token master for the test."""
    wl.init_db()
    init_symbol_db()
    meta_db.ensure_scanner_meta_tables_exists()

    sample_symbols = [
        {"symbol": "VNL", "exchange": "NSE", "token": "758000"},
        {"symbol": "NITYAS", "exchange": "NSE", "token": "758001"},
        {"symbol": "RKFAL-ST", "exchange": "NSE", "token": "766540"},
        {"symbol": "544982", "exchange": "BSE", "token": "544982"},
        {"symbol": "TCS", "exchange": "NSE", "token": "11536"},
        {"symbol": "RELIANCE", "exchange": "NSE", "token": "2885"},
    ]
    for s in sample_symbols:
        existing = (
            SymToken.query.filter_by(symbol=s["symbol"], exchange=s["exchange"]).first()
        )
        if not existing:
            row = SymToken(
                symbol=s["symbol"],
                brsymbol=s["symbol"],
                name=s["symbol"],
                exchange=s["exchange"],
                brexchange=s["exchange"],
                token=s["token"],
                instrumenttype="",
                tick_size=0.05,
                lotsize=1,
            )
            from database.symbol import db_session as sym_session
            sym_session.add(row)
            sym_session.commit()
    get_cache().load_all_symbols("angel")


@pytest.fixture(autouse=True)
def clean_slate(monkeypatch):
    """Ensure database is clean before and after each test."""
    _seed_test_symbols()

    # Mock precache background downloads to avoid external network delays
    monkeypatch.setattr(
        "services.watchlist_precache_service.trigger_watchlist_precache",
        lambda *args, **kwargs: True,
    )

    wl.db_session.rollback()
    for w in wl.get_watchlists(TEST_USER):
        if not wl.is_flagged_list_name(w["name"]):
            wl.delete_watchlist(TEST_USER, w["id"])
    wl.db_session.query(meta_db.ScannerStockProvenance).filter_by(user_id=TEST_USER).delete()
    wl.db_session.commit()

    yield

    wl.db_session.rollback()
    for w in wl.get_watchlists(TEST_USER):
        if not wl.is_flagged_list_name(w["name"]):
            wl.delete_watchlist(TEST_USER, w["id"])
    wl.db_session.query(meta_db.ScannerStockProvenance).filter_by(user_id=TEST_USER).delete()
    wl.db_session.commit()

    shutdown_ipo_scheduler()
    wl.db_session.remove()


@pytest.fixture
def app():
    """Create test Flask application with scanner gateway blueprint."""
    test_app = Flask(__name__)
    test_app.secret_key = "test-secret-key-openalgo-ipo"
    test_app.config["TESTING"] = True
    test_app.register_blueprint(scanner_gateway_bp)

    @test_app.teardown_appcontext
    def shutdown_sessions(exception=None):
        wl.db_session.remove()

    return test_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_fetch_trendlyne_ipos_returns_valid_dataset():
    """Verify scraper fetches live or fallback IPOs with required attributes."""
    items = fetch_trendlyne_ipos(year="2026")
    assert len(items) >= 200, f"Expected at least 200 IPOs, got {len(items)}"

    # Check that both Mainboard and SME items are present
    sme_count = sum(1 for it in items if it.get("is_sme"))
    mainboard_count = sum(1 for it in items if not it.get("is_sme"))

    assert sme_count > 0, "Expected SME items in discovery"
    assert mainboard_count > 0, "Expected Mainboard items in discovery"

    first = items[0]
    assert "symbol" in first
    assert "exchange" in first
    assert "is_sme" in first
    assert "metadata" in first


def test_sync_ipo_watchlists_creates_both_watchlists():
    """Verify sync_ipo_watchlists populates 'IPO 2026 - Mainboard' and 'IPO 2026 - SME'."""
    res = sync_ipo_watchlists(user_id=TEST_USER, action="append", year="2026")
    assert res["status"] == "success"
    assert res["total_discovered"] >= 200

    watchlists = [w for w in wl.get_watchlists(TEST_USER) if not wl.is_flagged_list_name(w["name"])]
    wl_by_name = {w["name"]: w for w in watchlists}

    assert "IPO 2026 - Mainboard" in wl_by_name
    assert "IPO 2026 - SME" in wl_by_name

    mb_wl = wl_by_name["IPO 2026 - Mainboard"]
    sme_wl = wl_by_name["IPO 2026 - SME"]

    assert len(mb_wl["items"]) > 0
    assert len(sme_wl["items"]) > 0

    # Verify provenance was saved
    provs = meta_db.get_active_provenance_symbols(TEST_USER, "IPO 2026 - Mainboard", "trendlyne_ipo_2026")
    assert len(provs) > 0


def test_source_scoped_replace_preserves_manual_additions():
    """Adding a manual stock to 'IPO 2026 - Mainboard' must not be deleted on subsequent replace sync."""
    # 1. First sync to populate watchlist
    res1 = sync_ipo_watchlists(user_id=TEST_USER, action="append", year="2026")
    assert res1["status"] == "success"

    # 2. Add manual stock
    mb_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "IPO 2026 - Mainboard"][0]
    wl.add_item(TEST_USER, mb_wl["id"], "TCS", "NSE")

    # Verify TCS is present
    mb_wl = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "IPO 2026 - Mainboard"][0]
    symbols = {it["symbol"] for it in mb_wl["items"]}
    assert "TCS" in symbols

    # 3. Run replace sync
    res2 = sync_ipo_watchlists(user_id=TEST_USER, action="replace", year="2026")
    assert res2["status"] == "success"

    # Verify TCS is STILL present after replace
    mb_wl_after = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "IPO 2026 - Mainboard"][0]
    symbols_after = {it["symbol"] for it in mb_wl_after["items"]}
    assert "TCS" in symbols_after, "Manual addition TCS must be preserved during source replace!"


def test_zero_result_safeguard(monkeypatch):
    """If external scraping fails completely and returns 0 items, watchlists must NOT be wiped."""
    import services.ipo_discovery_service as ipo_svc

    # 1. Seed watchlist first
    res1 = sync_ipo_watchlists(user_id=TEST_USER, action="append", year="2026")
    assert res1["status"] == "success"

    mb_before = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "IPO 2026 - Mainboard"][0]
    count_before = len(mb_before["items"])
    assert count_before > 0

    # 2. Mock scraper to simulate failed fetch (empty list)
    monkeypatch.setattr(ipo_svc, "fetch_trendlyne_ipos", lambda year: [])

    # 3. Run sync
    res2 = ipo_svc.sync_ipo_watchlists(user_id=TEST_USER, action="replace", year="2026")
    assert res2["status"] == "warning"
    assert "Existing watchlists preserved" in res2["message"]

    # 4. Verify watchlist was NOT wiped
    mb_after = [w for w in wl.get_watchlists(TEST_USER) if w["name"] == "IPO 2026 - Mainboard"][0]
    assert len(mb_after["items"]) == count_before


def test_trigger_ipo_sync_rest_endpoint(client, monkeypatch):
    """POST /api/v1/custom/ipo/sync should trigger on-demand sync for authenticated user."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(
        gw,
        "get_username_by_apikey",
        lambda key: TEST_USER if key == VALID_API_KEY else None,
    )

    resp = client.post(
        "/api/v1/custom/ipo/sync",
        headers={"X-API-KEY": VALID_API_KEY},
        json={"action": "append", "year": "2026"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "success"
    assert "mainboard" in data
    assert "sme" in data


def test_precache_status_rest_endpoint(client, monkeypatch):
    """GET /api/v1/custom/precache/status should return pre-cache worker status."""
    import blueprints.scanner_gateway as gw

    monkeypatch.setattr(gw, "get_username_by_apikey", lambda key: TEST_USER)

    resp = client.get(
        "/api/v1/custom/precache/status",
        headers={"X-API-KEY": VALID_API_KEY},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "success"
    assert "status" in data["data"]
    assert "completed" in data["data"]


def test_ipo_scheduler_initialization():
    """Verify scheduler registers 08:30 and 16:00 Asia/Kolkata cron triggers."""
    scheduler = init_ipo_scheduler()
    assert scheduler is not None
    assert scheduler.running

    jobs = scheduler.get_jobs()
    job_ids = [j.id for j in jobs]
    assert "ipo_sync_morning_0830" in job_ids
    assert "ipo_sync_evening_1600" in job_ids

    # Calling init again should be idempotent
    scheduler2 = init_ipo_scheduler()
    assert scheduler2 == scheduler

    shutdown_ipo_scheduler()
