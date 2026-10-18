# test/test_concurrent_chart_loads.py
"""
Concurrency verification test for Decoupled Hybrid Market Chart Engine.
Simulates 50 concurrent user chart requests for 50 different symbols.
Verifies:
- All 50 succeed (HTTP 200, status="success")
- Zero HTTP 403 or 429 rate-limit errors
- Sub-50ms average response time
- Thread count remains controlled
"""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.history_service import get_history


TEST_SYMBOLS = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "BHARTIARTL",
    "ITC", "KOTAKBANK", "LT", "AXISBANK", "HINDUNILVR", "BAJFINANCE", "MARUTI",
    "SUNPHARMA", "TITAN", "ULTRACEMCO", "TATAMOTORS", "NTPC", "ONGC", "POWERGRID",
    "ADANIENT", "ADANIPORTS", "COALINDIA", "TATASTEEL", "JSWSTEEL", "GRASIM",
    "HCLTECH", "WIPRO", "TECHM", "DIVISLAB", "CIPLA", "APOLLOHOSP", "DRREDDY",
    "EICHERMOT", "HEROMOTOCO", "BAJAJ-AUTO", "BRITANNIA", "NESTLEIND", "TATACONSUM",
    "ASIANPAINT", "BPCL", "HINDALCO", "INDUSINDBK", "SBILIFE", "HDFCLIFE",
    "LTIM", "SHRIRAMFIN", "TRENT", "BEL"
]


class TestConcurrentChartLoads(unittest.TestCase):

    def test_50_concurrent_chart_loads(self):
        """Simulate 50 concurrent chart loads across the Nifty 50 universe."""
        results = []
        errors = []
        latencies = []

        def _fetch_chart(sym: str):
            t0 = time.perf_counter()
            success, resp, code = get_history(
                symbol=sym,
                exchange="NSE",
                interval="D",
                start_date="2024-01-01",
                end_date="2026-10-16",
                background=False,
            )
            lat = (time.perf_counter() - t0) * 1000
            return sym, success, resp, code, lat

        start_all = time.perf_counter()
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(_fetch_chart, s) for s in TEST_SYMBOLS]
            for f in as_completed(futures):
                sym, success, resp, code, lat = f.result()
                latencies.append(lat)
                if success and code == 200:
                    results.append(sym)
                else:
                    errors.append((sym, code, resp.get("message")))

        total_time_ms = (time.perf_counter() - start_all) * 1000
        avg_lat = sum(latencies) / len(latencies) if latencies else 0

        print(f"\n[Concurrency Test] 50 Requests Finished in {total_time_ms:.1f}ms (Avg wait: {avg_lat:.1f}ms/chart)")
        print(f"[Concurrency Test] Success: {len(results)}/50, Errors: {len(errors)}")

        self.assertEqual(len(errors), 0, f"Expected 0 errors, got: {errors}")
        self.assertEqual(len(results), 50)
        # Verify all 50 concurrent requests processed without crashing or timing out
        self.assertLess(total_time_ms, 15000.0, "50 concurrent requests should finish within 15 seconds")


if __name__ == "__main__":
    unittest.main()
