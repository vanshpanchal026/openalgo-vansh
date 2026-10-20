# services/ipo_discovery_service.py
"""
Automated IPO Discovery and Dual Watchlist Synchronization Service.

Discovers Mainboard and SME IPOs (from Trendlyne or local fallback),
classifies them, and keeps two dedicated watchlists up-to-date:
  1. 'IPO 2026 - Mainboard'
  2. 'IPO 2026 - SME'

Scheduled automatically at 08:30 and 16:00 Asia/Kolkata.
Adheres to strict safeguards:
  - If Trendlyne scraping fails, existing watchlists are NEVER deleted or overwritten.
  - Multi-source provenance and manual additions are preserved.
  - Company fundamentals and issue data are stored in isolated research metadata.
  - Pre-caching of Daily candles is automatically triggered for new listings.
"""

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from blueprints.scanner_gateway import sync_watchlist_data
from database.user_db import User, db_session as user_db_session
from utils import real_threading
from utils.db_sessions import releases_scoped_sessions, remove_all_scoped_sessions
from utils.logging import get_logger

logger = get_logger(__name__)

# Constants
TRENDLYNE_URL_TEMPLATE = "https://trendlyne.com/ipo/screener/year-{year}/"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Concurrency lock to prevent overlapping sync runs
_sync_lock = real_threading.Lock()
_scheduler: Optional[BackgroundScheduler] = None
_scheduler_lock = real_threading.Lock()


def fetch_trendlyne_ipos(year: str = "2026") -> List[Dict[str, Any]]:
    """
    Fetch and parse IPO entries from Trendlyne for the specified year.
    Falls back gracefully to local workspace JSON files if the web request
    encounters network errors, bot protection, or returns zero rows.
    """
    year_str = str(year).strip()
    if not re.match(r"^20\d{2}$", year_str) or not (2000 <= int(year_str) <= 2030):
        logger.warning("[IPO Discovery] Invalid year format '%s'. Defaulting to 2026.", year)
        year_str = "2026"

    url = TRENDLYNE_URL_TEMPLATE.format(year=year_str)
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }

    scraped_items: List[Dict[str, Any]] = []

    try:
        logger.info("[IPO Discovery] Fetching live IPO screener from %s", url)
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            # Targeted search for __NEXT_DATA__ script or any script containing screenerRows
            match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, flags=re.DOTALL)
            script_matches = [match.group(1)] if match else re.findall(r"<script[^>]*>(.*?)</script>", resp.text, flags=re.DOTALL)
            for s in script_matches:
                if "screenerRows" in s:
                    try:
                        data = json.loads(s)
                        rows = data.get("props", {}).get("pageProps", {}).get("screenerRows", [])
                        for row in rows:
                            # Symbol determination
                            raw_sym = str(
                                row.get("stock_code")
                                or row.get("NSEcode")
                                or row.get("BSEScriptCode")
                                or ""
                            ).strip()
                            if not raw_sym:
                                continue

                            # Exchange determination
                            exch_flags = str(row.get("exchange_flags") or "").upper()
                            exch = "BSE" if ("BSE" in exch_flags and "NSE" not in exch_flags) else "NSE"

                            is_sme = bool(row.get("is_sme"))

                            # Extract research metadata
                            metadata = {
                                "company_name": row.get("company_name"),
                                "listing_date": row.get("listing_date"),
                                "issue_price": row.get("issue_price"),
                                "issue_size": row.get("issue_size"),
                                "is_sme": is_sme,
                                "subscription_stats": {
                                    "total": row.get("total_subscription"),
                                    "qib": row.get("qib"),
                                    "hni": row.get("hni"),
                                    "retail": row.get("retail"),
                                },
                                "raw_metadata": {
                                    "listing_open_price": row.get("listing_open_price"),
                                    "listing_close_price": row.get("listing_close_price"),
                                    "listing_gainP": row.get("listing_gainP"),
                                    "current_price": row.get("current_price"),
                                    "current_gainP": row.get("current_gainP"),
                                    "isin": row.get("isin"),
                                    "bse_code": row.get("BSEcode"),
                                    "source": "trendlyne_live",
                                },
                            }

                            scraped_items.append(
                                {
                                    "symbol": raw_sym,
                                    "exchange": exch,
                                    "is_sme": is_sme,
                                    "metadata": metadata,
                                }
                            )
                        break
                    except Exception as json_err:
                        logger.warning("[IPO Discovery] Error parsing JSON script: %s", json_err)

    except Exception as fetch_err:
        logger.warning(
            "[IPO Discovery] Trendlyne web fetch failed (%s). Falling back to local data.",
            fetch_err,
        )

    if scraped_items:
        logger.info(
            "[IPO Discovery] Successfully scraped %d IPOs from Trendlyne for year %s.",
            len(scraped_items),
            year_str,
        )
        return scraped_items

    # -------------------------------------------------------------
    # Fallback to local files if scraping returned zero items
    # -------------------------------------------------------------
    logger.info("[IPO Discovery] Using local fallback files for year %s", year_str)
    fallback_items: List[Dict[str, Any]] = []

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mb_candidates = [
        os.path.join(base_dir, f"ipo_{year_str}_mainboard_100.json"),
        f"ipo_{year_str}_mainboard_100.json",
    ]
    mb_file = next((p for p in mb_candidates if os.path.exists(p)), None)

    # 1. Mainboard fallback file
    if mb_file:
        try:
            with open(mb_file, "r", encoding="utf-8") as f:
                mb_data = json.load(f)
                for it in mb_data.get("items", []):
                    fallback_items.append(
                        {
                            "symbol": it.get("symbol"),
                            "exchange": it.get("exchange", "NSE"),
                            "is_sme": False,
                            "metadata": {
                                "is_sme": False,
                                "raw_metadata": {"source": "local_fallback_json"},
                            },
                        }
                    )
        except Exception as e:
            logger.warning("[IPO Discovery] Could not read %s: %s", mb_file, e)

    # 2. SME fallback file
    sme_candidates = [
        os.path.join(base_dir, f"ipo_{year_str}_sme_183.json"),
        f"ipo_{year_str}_sme_183.json",
    ]
    sme_file = next((p for p in sme_candidates if os.path.exists(p)), None)
    if sme_file:
        try:
            with open(sme_file, "r", encoding="utf-8") as f:
                sme_data = json.load(f)
                for it in sme_data.get("items", []):
                    fallback_items.append(
                        {
                            "symbol": it.get("symbol"),
                            "exchange": it.get("exchange", "NSE"),
                            "is_sme": True,
                            "metadata": {
                                "is_sme": True,
                                "raw_metadata": {"source": "local_fallback_json"},
                            },
                        }
                    )
        except Exception as e:
            logger.warning("[IPO Discovery] Could not read %s: %s", sme_file, e)

    logger.info(
        "[IPO Discovery] Loaded %d items from local fallback files.", len(fallback_items)
    )
    return fallback_items


def sync_ipo_watchlists(
    user_id: Optional[str] = None,
    action: str = "append",
    year: str = "2026",
) -> Dict[str, Any]:
    """
    Synchronizes newly discovered IPOs into two dedicated watchlists:
      - 'IPO <year> - Mainboard'
      - 'IPO <year> - SME'

    Safeguards:
      - If no IPOs are discovered, existing watchlists are left untouched.
      - Never runs concurrently; uses an atomic lock.
    """
    if not _sync_lock.acquire(blocking=False):
        logger.warning("[IPO Discovery] Sync already in progress, skipping concurrent call.")
        return {
            "status": "skipped",
            "message": "Another IPO sync is already running in background",
        }

    try:
        # Resolve user safely without hardcoded identities
        if not user_id:
            user_row = user_db_session.query(User.username).first()
            if user_row:
                user_id = user_row[0]
            else:
                logger.error("[IPO Discovery] No registered users found in user database.")
                return {
                    "status": "error",
                    "message": "No registered users found in user database.",
                }

        logger.info(
            "[IPO Discovery] Starting sync for user '%s', action='%s', year='%s'",
            user_id,
            action,
            year,
        )

        all_ipos = fetch_trendlyne_ipos(year=year)
        if not all_ipos:
            logger.error("[IPO Discovery] No IPO items found. Existing watchlists preserved.")
            return {
                "status": "warning",
                "message": "Zero IPO items discovered. Existing watchlists preserved without change.",
            }

        # Classify Mainboard vs SME
        mainboard_items = [
            {"symbol": it["symbol"], "exchange": it["exchange"], "metadata": it.get("metadata")}
            for it in all_ipos
            if not it.get("is_sme")
        ]

        sme_items = [
            {"symbol": it["symbol"], "exchange": it["exchange"], "metadata": it.get("metadata")}
            for it in all_ipos
            if it.get("is_sme")
        ]

        source_id = f"trendlyne_ipo_{year}"
        mainboard_wl = f"IPO {year} - Mainboard"
        sme_wl = f"IPO {year} - SME"

        # 1. Sync Mainboard Watchlist
        ok_mb, res_mb, code_mb = sync_watchlist_data(
            user_id=user_id,
            watchlist_name=mainboard_wl,
            source=source_id,
            raw_items=mainboard_items,
            action=action,
            position_pref="top",
        )

        # 2. Sync SME Watchlist
        ok_sme, res_sme, code_sme = sync_watchlist_data(
            user_id=user_id,
            watchlist_name=sme_wl,
            source=source_id,
            raw_items=sme_items,
            action=action,
            position_pref="top",
        )

        success = ok_mb and ok_sme
        logger.info(
            "[IPO Discovery] Sync complete for user '%s': Mainboard (%s: +%d), SME (%s: +%d)",
            user_id,
            "OK" if ok_mb else "ERR",
            res_mb.get("summary", {}).get("valid_added", 0),
            "OK" if ok_sme else "ERR",
            res_sme.get("summary", {}).get("valid_added", 0),
        )

        return {
            "status": "success" if success else "partial_error",
            "year": year,
            "user_id": user_id,
            "action": action,
            "total_discovered": len(all_ipos),
            "mainboard": {
                "watchlist_name": mainboard_wl,
                "status_code": code_mb,
                "result": res_mb,
            },
            "sme": {
                "watchlist_name": sme_wl,
                "status_code": code_sme,
                "result": res_sme,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }

    finally:
        _sync_lock.release()
        remove_all_scoped_sessions()


@releases_scoped_sessions
def _scheduled_sync_job() -> None:
    """Scheduled task executed at 08:30 and 16:00 Asia/Kolkata across all registered users."""
    logger.info("[IPO Scheduler] Starting scheduled IPO sync job...")
    try:
        users = [u.username for u in user_db_session.query(User.username).all()]
        if not users:
            logger.warning("[IPO Scheduler] No users found for scheduled sync.")
            return

        for user in users:
            try:
                res = sync_ipo_watchlists(user_id=user, action="append", year="2026")
                mb_added = (
                    res.get("mainboard", {}).get("result", {}).get("summary", {}).get("valid_added", 0)
                )
                sme_added = res.get("sme", {}).get("result", {}).get("summary", {}).get("valid_added", 0)
                logger.info(
                    "[IPO Scheduler] User '%s' IPO sync finished: Mainboard +%d, SME +%d",
                    user,
                    mb_added,
                    sme_added,
                )
            except Exception as user_err:
                logger.error("[IPO Scheduler] Sync failed for user '%s': %s", user, user_err)
    except Exception as e:
        logger.exception("[IPO Scheduler] Unhandled error during scheduled IPO sync: %s", e)


def init_ipo_scheduler() -> Optional[BackgroundScheduler]:
    """
    Initialize background scheduler for IPO discovery at 08:30 and 16:00 Asia/Kolkata.
    Guarantees thread-safety and idempotent start.
    """
    global _scheduler
    with _scheduler_lock:
        if _scheduler and _scheduler.running:
            return _scheduler

        try:
            _scheduler = BackgroundScheduler(daemon=True)
            # 08:30 Asia/Kolkata (morning before market opens)
            _scheduler.add_job(
                _scheduled_sync_job,
                trigger=CronTrigger(hour=8, minute=30, timezone="Asia/Kolkata"),
                id="ipo_sync_morning_0830",
                name="IPO Sync Morning (08:30 IST)",
                replace_existing=True,
            )
            # 16:00 Asia/Kolkata (after market close)
            _scheduler.add_job(
                _scheduled_sync_job,
                trigger=CronTrigger(hour=16, minute=0, timezone="Asia/Kolkata"),
                id="ipo_sync_evening_1600",
                name="IPO Sync Evening (16:00 IST)",
                replace_existing=True,
            )
            _scheduler.start()
            logger.info(
                "[IPO Scheduler] Started successfully with triggers at 08:30 & 16:00 Asia/Kolkata."
            )
            return _scheduler
        except Exception as e:
            logger.error("[IPO Scheduler] Failed to initialize: %s", e)
            return None


def shutdown_ipo_scheduler() -> None:
    """Graceful shutdown of the IPO background scheduler."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler and _scheduler.running:
            _scheduler.shutdown(wait=False)
            _scheduler = None
            logger.info("[IPO Scheduler] Stopped successfully.")
