# test/test_live_candle_aggregator.py
import time
import unittest
from services.live_candle_aggregator import LiveCandleAggregator, live_aggregator
from services.history_service import _merge_live_forming_bar


class TestLiveCandleAggregator(unittest.TestCase):

    def setUp(self):
        self.agg = LiveCandleAggregator()

    def test_forming_candle_aggregation(self):
        # Anchor now_ts safely at second 5 of a minute so +15s never crosses a minute boundary
        now_ts = (int(time.time()) // 300) * 300 + 5
        sym = "TCS"
        exch = "NSE"

        # Ticks: 100 -> 105 -> 95 -> 102
        self.agg.on_tick(sym, exch, 100.0, volume=10, epoch_sec=now_ts)
        self.agg.on_tick(sym, exch, 105.0, volume=25, epoch_sec=now_ts + 5)
        self.agg.on_tick(sym, exch, 95.0, volume=40, epoch_sec=now_ts + 10)
        self.agg.on_tick(sym, exch, 102.0, volume=50, epoch_sec=now_ts + 15)

        # 1-minute forming bar
        bar_1m = self.agg.get_forming_candle(sym, exch, "1m")
        self.assertIsNotNone(bar_1m)
        self.assertEqual(bar_1m["open"], 100.0)
        self.assertEqual(bar_1m["high"], 105.0)
        self.assertEqual(bar_1m["low"], 95.0)
        self.assertEqual(bar_1m["close"], 102.0)
        self.assertEqual(bar_1m["volume"], 50)

        # Daily forming bar
        bar_d = self.agg.get_forming_candle(sym, exch, "D")
        self.assertIsNotNone(bar_d)
        self.assertEqual(bar_d["open"], 100.0)
        self.assertEqual(bar_d["high"], 105.0)
        self.assertEqual(bar_d["low"], 95.0)
        self.assertEqual(bar_d["close"], 102.0)

    def test_merge_live_forming_bar(self):
        sym = "INFY"
        exch = "NSE"
        now_ts = int(time.time())
        minute_ts = (now_ts // 60) * 60

        live_aggregator.on_tick(sym, exch, 1500.0, volume=100, epoch_sec=now_ts)
        live_aggregator.on_tick(sym, exch, 1520.0, volume=200, epoch_sec=now_ts + 2)

        # Test 1: Historical records end prior to forming bar
        hist_records = [
            {"timestamp": minute_ts - 120, "open": 1490.0, "high": 1495.0, "low": 1485.0, "close": 1492.0, "volume": 50, "oi": 0},
            {"timestamp": minute_ts - 60, "open": 1492.0, "high": 1505.0, "low": 1490.0, "close": 1500.0, "volume": 80, "oi": 0},
        ]
        merged = _merge_live_forming_bar(list(hist_records), sym, exch, "1m")
        self.assertEqual(len(merged), 3)
        self.assertEqual(merged[-1]["timestamp"], minute_ts)
        self.assertEqual(merged[-1]["close"], 1520.0)

        # Test 2: Last historical bar matches forming timestamp (update in place)
        hist_records_matching = [
            {"timestamp": minute_ts, "open": 1498.0, "high": 1510.0, "low": 1498.0, "close": 1505.0, "volume": 150, "oi": 0},
        ]
        merged_match = _merge_live_forming_bar(list(hist_records_matching), sym, exch, "1m")
        self.assertEqual(len(merged_match), 1)
        self.assertEqual(merged_match[0]["high"], 1520.0)
        self.assertEqual(merged_match[0]["close"], 1520.0)


if __name__ == "__main__":
    unittest.main()
