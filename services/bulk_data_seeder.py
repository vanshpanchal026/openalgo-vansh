# services/bulk_data_seeder.py
"""
TejHQ Bulk EOD Lakehouse Seeder for OpenAlgo.

Populates multi-year Daily OHLCV data for all ~2,800 NSE & BSE stocks
in a few bulk downloads using official SEBI CMTS Bhavcopies published by TejHQ
(tejhq/indian-markets on HuggingFace).
Requires ZERO broker API calls, zero rate limits, and renders 500-stock watchlists instantly.
"""

import os
import threading
import time
from datetime import datetime
from typing import Any

import httpx

from database.historify_db import get_connection, get_db_path
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)

_seeder_lock = threading.Lock()
_seeder_thread: threading.Thread | None = None
_seed_status: dict[str, Any] = {
    "status": "idle",
    "completed_years": [],
    "total_records": 0,
    "last_run_at": None,
    "error": None,
}

BASE_HF_URL = "https://huggingface.co/datasets/tejhq/indian-markets/resolve/main"


def get_bulk_seed_status() -> dict[str, Any]:
    """Get current status of bulk lakehouse seeder."""
    with _seeder_lock:
        return dict(_seed_status)


def _get_lakehouse_cache_dir() -> str:
    """Ensure db/lakehouse directory exists."""
    db_file = get_db_path()
    base_dir = os.path.dirname(db_file)
    lh_dir = os.path.join(base_dir, "lakehouse")
    os.makedirs(lh_dir, exist_ok=True)
    return lh_dir


def seed_eod_from_tejhq(
    years: list[int] | None = None,
    exchanges: list[str] | None = None,
    force_download: bool = False,
    socketio: Any | None = None,
) -> dict[str, Any]:
    """
    Downloads yearly Parquet partitions from TejHQ and bulk-inserts them into DuckDB market_data.
    Default: Last 3 years (2024, 2025, 2026) for NSE.
    """
    global _seed_status

    if years is None:
        curr_year = datetime.now().year
        years = [curr_year - 2, curr_year - 1, curr_year]

    if exchanges is None:
        exchanges = ["NSE"]

    with _seeder_lock:
        _seed_status["status"] = "running"
        _seed_status["error"] = None
        _seed_status["started_at"] = datetime.now().isoformat()

    client = httpx.Client(follow_redirects=True)
    cache_dir = _get_lakehouse_cache_dir()
    total_inserted = 0
    completed: list[str] = []

    try:
        for exch in exchanges:
            exch_upper = exch.upper()
            exch_lower = exch.lower()

            for yr in years:
                partition_key = f"{exch_upper}_{yr}"
                file_name = f"{exch_lower}_{yr}.parquet"
                local_path = os.path.join(cache_dir, file_name)
                remote_url = f"{BASE_HF_URL}/{exch_lower}/year={yr}/{file_name}"

                # Download only if missing or forced
                if force_download or not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
                    logger.info(f"[Lakehouse Seeder] Downloading {partition_key} from {remote_url}...")
                    resp = client.get(remote_url, timeout=60.0)
                    if resp.status_code == 200:
                        with open(local_path, "wb") as f:
                            f.write(resp.content)
                        logger.info(f"[Lakehouse Seeder] Saved {local_path} ({len(resp.content) // 1024} KB).")
                    else:
                        logger.warning(f"[Lakehouse Seeder] Partition {partition_key} returned HTTP {resp.status_code}.")
                        continue

                # Vectorized DuckDB Ingestion
                norm_path = local_path.replace("\\", "/")
                logger.info(f"[Lakehouse Seeder] Ingesting {norm_path} into DuckDB market_data...")
                with get_connection() as conn:
                    # Ingest daily OHLCV
                    conn.execute(f"""
                        INSERT OR IGNORE INTO market_data (
                            symbol, exchange, interval, timestamp,
                            open, high, low, close, volume, oi, created_at
                        )
                        SELECT
                            UPPER(symbol),
                            '{exch_upper}',
                            'D',
                            EPOCH(CAST(date AS TIMESTAMP) - INTERVAL 330 MINUTE),
                            open,
                            high,
                            low,
                            close,
                            CAST(volume AS BIGINT),
                            0,
                            current_timestamp
                        FROM read_parquet('{norm_path}')
                        WHERE symbol IS NOT NULL;
                    """)
                    row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{norm_path}') WHERE symbol IS NOT NULL;").fetchone()[0]
                    total_inserted += row_count
                    completed.append(partition_key)
                    logger.info(f"[Lakehouse Seeder] Finished {partition_key}: {row_count} bars processed.")

        # Rebuild/update catalog for daily bars in bulk
        if completed:
            with get_connection() as conn:
                conn.execute("""
                    DELETE FROM data_catalog WHERE interval = 'D';
                    INSERT INTO data_catalog (
                        id, symbol, exchange, interval, first_timestamp, last_timestamp, record_count, last_download_at
                    )
                    SELECT
                        nextval('data_catalog_id_seq'),
                        symbol,
                        exchange,
                        'D',
                        MIN(timestamp),
                        MAX(timestamp),
                        COUNT(*),
                        current_timestamp
                    FROM market_data
                    WHERE interval = 'D'
                    GROUP BY symbol, exchange;
                """)

        with _seeder_lock:
            _seed_status["status"] = "completed"
            _seed_status["completed_years"] = completed
            _seed_status["total_records"] = total_inserted
            _seed_status["last_run_at"] = datetime.now().isoformat()

        logger.info(
            f"[Lakehouse Seeder] Full EOD Ingestion Complete! {total_inserted} records imported across {completed}."
        )

        if socketio:
            try:
                socketio.emit(
                    "toast",
                    {
                        "type": "success",
                        "title": "Market Lakehouse Synced",
                        "message": f"Bulk EOD Synced: {total_inserted:,} bars ready across ~2,800 symbols.",
                    },
                )
            except Exception as e:
                logger.debug(f"[Lakehouse Seeder] Could not emit socketio toast: {e}")

        return {
            "status": "success",
            "inserted_records": total_inserted,
            "completed_partitions": completed,
        }

    except Exception as e:
        logger.exception(f"[Lakehouse Seeder] Error during bulk EOD ingestion: {e}")
        with _seeder_lock:
            _seed_status["status"] = "failed"
            _seed_status["error"] = str(e)
            _seed_status["last_run_at"] = datetime.now().isoformat()
        return {"status": "error", "message": str(e)}


def trigger_bulk_seed_async(
    years: list[int] | None = None,
    exchanges: list[str] | None = None,
    force: bool = False,
    socketio: Any | None = None,
) -> bool:
    """
    Triggers the bulk lakehouse seeder in a non-blocking daemon thread.
    Returns True if started, False if already running.
    """
    global _seeder_thread

    with _seeder_lock:
        if _seeder_thread is not None and _seeder_thread.is_alive():
            logger.info("[Lakehouse Seeder] Seeder already running. Skipping duplicate trigger.")
            return False

        _seeder_thread = threading.Thread(
            target=seed_eod_from_tejhq,
            kwargs={
                "years": years,
                "exchanges": exchanges,
                "force_download": force,
                "socketio": socketio,
            },
            name="LakehouseBulkSeeder",
            daemon=True,
        )
        _seeder_thread.start()
        return True
