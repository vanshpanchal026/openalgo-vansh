# services/instrument_master_service.py
"""
Unified Cross-Broker Instrument Master Service.

Ingests daily Beginning-of-Day (BOD) scrip masters from Upstox and Angel One
into local DuckDB `instrument_master` table to provide sub-millisecond,
unified symbol-to-key resolution across NSE, BSE, SME series (-SM/-ST), and Indices.
"""

import gzip
import json
import os
import threading
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from database.historify_db import get_instrument_count, resolve_instrument_keys, upsert_instrument_master_df
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)

_sync_lock = threading.Lock()
_last_sync_date: str | None = None

UPSTOX_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
UPSTOX_BSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"


def sync_unified_master(force: bool = False) -> dict[str, Any]:
    """
    Downloads and parses Upstox NSE & BSE instruments, maps symbols and ISINs,
    and populates DuckDB's `instrument_master` table.
    Guarded to run at most once per calendar day unless force=True.
    """
    global _last_sync_date

    today_str = datetime.now().strftime("%Y-%m-%d")
    with _sync_lock:
        if not force and _last_sync_date == today_str:
            count = get_instrument_count()
            if count > 0:
                return {"status": "skipped", "message": "Already synced today", "count": count}

        logger.info("[InstrumentMaster] Starting unified instrument master sync...")
        client = get_httpx_client()
        records: list[dict[str, Any]] = []

        # 1. Process Upstox NSE
        try:
            resp_nse = client.get(UPSTOX_NSE_URL, timeout=30.0)
            if resp_nse.status_code == 200:
                raw_data = json.loads(gzip.decompress(resp_nse.content))
                for item in raw_data:
                    segment = item.get("segment")
                    if segment not in ("NSE_EQ", "NSE_INDEX"):
                        continue
                    tsym = (item.get("trading_symbol") or "").strip().upper()
                    if not tsym:
                        continue
                    
                    inst_type = (item.get("instrument_type") or "EQ").strip().upper()
                    isin = item.get("isin")
                    inst_key = item.get("instrument_key")
                    token = str(item.get("exchange_token") or "")
                    lot = int(item.get("lot_size") or 1)
                    tick = float(item.get("tick_size") or 0.05)

                    # Build clean vs display symbol
                    clean = tsym
                    if inst_type in ("SM", "ST"):
                        display = f"{tsym}-{inst_type}"
                    elif inst_type == "INDEX":
                        display = tsym
                        clean = tsym
                    else:
                        display = f"{tsym}-EQ"

                    records.append({
                        "clean_symbol": clean,
                        "display_symbol": display,
                        "exchange": "NSE",
                        "series": inst_type,
                        "isin": isin,
                        "angel_token": token,
                        "angel_symbol": display,
                        "upstox_key": inst_key,
                        "lot_size": lot,
                        "tick_size": tick,
                    })
                    # Also register bare trading symbol as an alternate lookup
                    if display != tsym:
                        records.append({
                            "clean_symbol": clean,
                            "display_symbol": tsym,
                            "exchange": "NSE",
                            "series": inst_type,
                            "isin": isin,
                            "angel_token": token,
                            "angel_symbol": display,
                            "upstox_key": inst_key,
                            "lot_size": lot,
                            "tick_size": tick,
                        })
                logger.info(f"[InstrumentMaster] Processed {len(records)} NSE cash/index scrips.")
        except Exception as e:
            logger.warning(f"[InstrumentMaster] Error processing NSE scrip master: {e}")

        # 2. Process Upstox BSE
        try:
            resp_bse = client.get(UPSTOX_BSE_URL, timeout=30.0)
            if resp_bse.status_code == 200:
                raw_bse = json.loads(gzip.decompress(resp_bse.content))
                bse_count = 0
                for item in raw_bse:
                    segment = item.get("segment")
                    if segment not in ("BSE_EQ", "BSE_INDEX"):
                        continue
                    tsym = (item.get("trading_symbol") or "").strip().upper()
                    if not tsym:
                        continue
                    
                    inst_type = (item.get("instrument_type") or "A").strip().upper()
                    isin = item.get("isin")
                    inst_key = item.get("instrument_key")
                    token = str(item.get("exchange_token") or "")
                    lot = int(item.get("lot_size") or 1)
                    tick = float(item.get("tick_size") or 0.05)

                    records.append({
                        "clean_symbol": tsym,
                        "display_symbol": tsym,
                        "exchange": "BSE",
                        "series": inst_type,
                        "isin": isin,
                        "angel_token": token,
                        "angel_symbol": tsym,
                        "upstox_key": inst_key,
                        "lot_size": lot,
                        "tick_size": tick,
                    })
                    bse_count += 1
                logger.info(f"[InstrumentMaster] Processed {bse_count} BSE cash/index scrips.")
        except Exception as e:
            logger.warning(f"[InstrumentMaster] Error processing BSE scrip master: {e}")

        # 3. Add explicit well-known Index aliases
        well_known_indices = [
            {"clean": "NIFTY", "display": "NIFTY", "exch": "NSE", "key": "NSE_INDEX|Nifty 50", "token": "26000"},
            {"clean": "NIFTY 50", "display": "NIFTY 50", "exch": "NSE", "key": "NSE_INDEX|Nifty 50", "token": "26000"},
            {"clean": "BANKNIFTY", "display": "BANKNIFTY", "exch": "NSE", "key": "NSE_INDEX|Nifty Bank", "token": "26009"},
            {"clean": "NIFTY BANK", "display": "NIFTY BANK", "exch": "NSE", "key": "NSE_INDEX|Nifty Bank", "token": "26009"},
            {"clean": "FINNIFTY", "display": "FINNIFTY", "exch": "NSE", "key": "NSE_INDEX|Nifty Fin Service", "token": "26037"},
            {"clean": "SENSEX", "display": "SENSEX", "exch": "BSE", "key": "BSE_INDEX|SENSEX", "token": "1"},
        ]
        for idx in well_known_indices:
            records.append({
                "clean_symbol": idx["clean"],
                "display_symbol": idx["display"],
                "exchange": idx["exch"],
                "series": "INDEX",
                "isin": None,
                "angel_token": idx["token"],
                "angel_symbol": idx["display"],
                "upstox_key": idx["key"],
                "lot_size": 1,
                "tick_size": 0.05,
            })

        if records:
            df = pd.DataFrame(records)
            df = df.drop_duplicates(subset=["exchange", "display_symbol"], keep="first")
            inserted = upsert_instrument_master_df(df)
            _last_sync_date = today_str
            logger.info(f"[InstrumentMaster] Successfully populated {inserted} instruments into DuckDB.")
            return {"status": "success", "count": inserted}

        return {"status": "error", "message": "No instruments fetched"}


def sync_unified_master_async(force: bool = False) -> threading.Thread:
    """Run sync_unified_master in a background thread."""
    t = threading.Thread(
        target=sync_unified_master,
        args=(force,),
        name="UnifiedInstrumentMasterSync",
        daemon=True,
    )
    t.start()
    return t

