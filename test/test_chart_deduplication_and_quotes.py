import unittest
from database.historify_db import get_ohlcv
from database.auth_db import get_first_available_api_key
from services.quotes_service import get_lakehouse_quotes, get_multiquotes


class TestChartDeduplicationAndQuotes(unittest.TestCase):
    def test_daily_candles_no_duplicates(self):
        """Verify that daily candles have no same-day duplicates (Bug 1 fix)."""
        df = get_ohlcv("SIGMA", "NSE", "D")
        self.assertFalse(df.empty, "SIGMA NSE daily candles should not be empty")

        # Map each timestamp to IST calendar day: (ts + 19800) // 86400
        days = (df["timestamp"] + 19800) // 86400
        duplicate_count = days.duplicated().sum()
        self.assertEqual(duplicate_count, 0, f"Found {duplicate_count} duplicate daily candles for SIGMA NSE!")

    def test_lakehouse_quotes_fallback(self):
        """Verify lakehouse quotes return valid data for watchlist symbols (Bug 2 fix)."""
        symbols = [
            {"symbol": "SIGMA", "exchange": "NSE"},
            {"symbol": "SIGMA", "exchange": "BSE"},
            {"symbol": "ADON", "exchange": "BSE"},
            {"symbol": "APS-SM", "exchange": "NSE"},
        ]
        quotes = get_lakehouse_quotes(symbols)
        self.assertEqual(len(quotes), 4)

        for q in quotes:
            self.assertIn("data", q, f"Symbol {q['symbol']} should have quote data")
            data = q["data"]
            self.assertGreater(data["ltp"], 0, f"LTP should be positive for {q['symbol']}")
            self.assertGreater(data["prev_close"], 0, f"prev_close should be positive for {q['symbol']}")

    def test_multiquotes_lakehouse_fallback(self):
        """Verify get_multiquotes succeeds and returns quotes even without active broker."""
        apikey = get_first_available_api_key()
        self.assertIsNotNone(apikey, "An API key should be available in the test environment")

        symbols = [
            {"symbol": "SIGMA", "exchange": "NSE"},
            {"symbol": "ADON", "exchange": "BSE"},
        ]
        success, resp, status = get_multiquotes(symbols, api_key=apikey)
        self.assertTrue(success)
        self.assertEqual(status, 200)
        self.assertIn("results", resp)
        self.assertGreaterEqual(len(resp["results"]), 2)
        for r in resp["results"]:
            self.assertIsNotNone(r.get("data"), f"Expected data for {r.get('symbol')}")
            self.assertGreater(r["data"]["ltp"], 0)


if __name__ == "__main__":
    unittest.main()
