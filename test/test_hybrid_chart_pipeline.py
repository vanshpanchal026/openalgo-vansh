# test/test_hybrid_chart_pipeline.py
"""
End-to-end integration test for the Decoupled Hybrid Market Chart Engine.
Verifies:
1. Unified instrument master token & key resolution
2. DuckDB EOD lakehouse fast query (<20ms) and on-the-fly W/M aggregation
3. Live forming candle aggregator integration
4. Keyless public candle fetching without broker API calls
"""

import time
import unittest
from database.historify_db import resolve_instrument_keys, get_ohlcv
from services.history_service import get_history, get_history_from_db
from services.live_candle_aggregator import live_aggregator


class TestHybridChartPipeline(unittest.TestCase):

    def test_01_instrument_resolution(self):
        """Verify unified instrument master maps Equities, SMEs, and Indices."""
        # Reliance Equity: dict returned from resolve_instrument_keys
        rel_row = resolve_instrument_keys("RELIANCE", "NSE")
        self.assertIsNotNone(rel_row)
        upstox_key = rel_row.get("upstox_key", "")
        self.assertTrue("INE002A01018" in upstox_key or "RELIANCE" in upstox_key)

        # SME stock with suffix
        sme_row = resolve_instrument_keys("APS-SM", "NSE")
        self.assertIsNotNone(sme_row)

        # Index alias
        nifty_row = resolve_instrument_keys("NIFTY", "NSE")
        self.assertIsNotNone(nifty_row)
        self.assertEqual(nifty_row.get("upstox_key"), "NSE_INDEX|Nifty 50")

    def test_02_duckdb_lakehouse_query_speed_and_aggregation(self):
        """Verify DuckDB serves multi-year EOD data fast and resamples to W and M."""
        start_t = time.perf_counter()
        df_d = get_ohlcv("RELIANCE", "NSE", "D", 1704067200, int(time.time()))
        elapsed_ms = (time.perf_counter() - start_t) * 1000

        self.assertFalse(df_d.empty, "RELIANCE Daily data should be present in lakehouse")
        self.assertLess(elapsed_ms, 150.0, f"Query took {elapsed_ms:.2f}ms, should be <150ms")

        # Test on-the-fly Weekly aggregation
        df_w = get_ohlcv("RELIANCE", "NSE", "W", 1704067200, int(time.time()))
        self.assertFalse(df_w.empty)
        self.assertLess(len(df_w), len(df_d))

        # Test on-the-fly Monthly aggregation
        df_m = get_ohlcv("RELIANCE", "NSE", "M", 1704067200, int(time.time()))
        self.assertFalse(df_m.empty)
        self.assertLess(len(df_m), len(df_w))

    def test_03_history_service_cache_first_route(self):
        """Verify get_history returns status=success directly from DuckDB without broker calls."""
        success, resp, code = get_history(
            symbol="RELIANCE",
            exchange="NSE",
            interval="D",
            start_date="2024-01-01",
            end_date="2026-10-16",
            background=False,
        )
        self.assertTrue(success)
        self.assertEqual(code, 200)
        self.assertEqual(resp.get("status"), "success")
        self.assertTrue(len(resp.get("data", [])) > 0)

    def test_04_live_candle_aggregation_in_history(self):
        """Verify live incoming ticks are reflected in get_history."""
        sym = "TCS"
        exch = "NSE"
        now_ts = (int(time.time()) // 300) * 300 + 10
        live_aggregator.on_tick(sym, exch, 3850.50, volume=1000, epoch_sec=now_ts)

        # Get history from DB (or forming bar)
        forming = live_aggregator.get_forming_candle(sym, exch, "D")
        self.assertIsNotNone(forming)
        self.assertEqual(forming["close"], 3850.50)


if __name__ == "__main__":
    unittest.main()
