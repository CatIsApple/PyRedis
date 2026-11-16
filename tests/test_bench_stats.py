"""통계 함수(percentile, summarize)와 벤치마크 인자 파싱·마크다운 출력 테스트."""
import unittest

from bench.benchmark import format_markdown, parse_args
from bench.stats import percentile, summarize


class StatsTest(unittest.TestCase):
    def test_nearest_rank_percentile(self):
        samples = list(range(1, 101))
        self.assertEqual(percentile(samples, 50), 50)
        self.assertEqual(percentile(samples, 95), 95)
        self.assertIsNone(percentile([], 50))

    def test_summarize(self):
        s = summarize([1.0, 2.0, 3.0])
        self.assertEqual((s["count"], s["min"], s["max"], s["p50"]), (3, 1.0, 3.0, 2.0))
        self.assertEqual(summarize([])["count"], 0)


class BenchArgsTest(unittest.TestCase):
    def test_defaults_and_options(self):
        o = parse_args(["--spawn", "--requests", "5", "--concurrency", "1,4", "--no-save"])
        self.assertEqual((o["spawn"], o["requests"], o["concurrency"], o["save"]), (True, 5, [1, 4], False))

    def test_invalid_values_raise(self):
        with self.assertRaises(ValueError):
            parse_args(["--requests", "0"])
        with self.assertRaises(ValueError):
            parse_args(["--unknown"])
        with self.assertRaises(ValueError):
            parse_args(["--requests"])

    def test_markdown_has_table_rows(self):
        result = {
            "measuredAt": "2026-01-01T00:00:00.000Z",
            "command": "x",
            "options": {"requests": 2, "warmup": 0, "concurrency": [1]},
            "env": {"python": "3", "os": "o", "cpu": "c", "cores": 1, "dbRows": 10, "dbIndex": "off", "delayMs": 0},
            "rows": [{"label": "L", "concurrency": 1, "client": summarize([1.0, 2.0]), "server": summarize([0.5]), "throughput": 10.0, "unexpected": 0}],
        }
        md = format_markdown(result)
        self.assertIn("| L | 1 | 1.50 |", md)
        self.assertIn("| DB 행 수 | 10 |", md)


if __name__ == "__main__":
    unittest.main()
