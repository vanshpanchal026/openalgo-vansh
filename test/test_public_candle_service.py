# test/test_public_candle_service.py
"""Unit tests for Public Keyless Candle Service and DuckDB Resampling."""

import unittest
from database.historify_db import get_ohlcv, init_database, resolve_instrument_keys
from services.instrument_master_service import sync_unified_master
from services.public_candle_service import fetch_public_candles


class TestPublicCandleService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_database()
        sync_unified_master()

    def test_01_resolve_keys(self):
        resolved = resolve_instrument_keys("RELIANCE", "NSE")
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["clean_symbol"], "RELIANCE")
        self.assertIn("INE002A01018", resolved["upstox_key"])

        resolved_nifty = resolve_instrument_keys("NIFTY", "NSE")
        self.assertIsNotNone(resolved_nifty)
        self.assertEqual(resolved_nifty["upstox_key"], "NSE_INDEX|Nifty 50")

    def test_02_fetch_public_daily_candles(self):
        success, rows, err = fetch_public_candles("RELIANCE", "NSE", "D")
        self.assertTrue(success, f"Public candle fetch failed: {err}")
        self.assertGreater(len(rows), 0, "Expected at least 1 daily candle returned")
        # Verify columns exist
        sample = rows[0]
        for col in ("timestamp", "open", "high", "low", "close", "volume"):
            self.assertIn(col, sample)

    def test_03_fetch_public_intraday_nifty(self):
        success, rows, err = fetch_public_candles("NIFTY", "NSE", "1m")
        self.assertTrue(success, f"Public 1m fetch failed: {err}")
        self.assertGreater(len(rows), 0, "Expected 1m candles for NIFTY")

        # Verify DuckDB on-the-fly aggregation to 5m and 15m
        df_5m = get_ohlcv("NIFTY", "NSE", "5m")
        self.assertFalse(df_5m.empty, "DuckDB 5m aggregated dataframe should not be empty")

        df_15m = get_ohlcv("NIFTY", "NSE", "15m")
        self.assertFalse(df_15m.empty, "DuckDB 15m aggregated dataframe should not be empty")


if __name__ == "__main__":
    unittest.main()
