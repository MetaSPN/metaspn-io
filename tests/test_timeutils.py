import unittest

from metaspn_io.timeutils import parse_timestamp


class TimestampTests(unittest.TestCase):
    def test_normalizes_z_to_utc(self) -> None:
        ts, tz = parse_timestamp("2026-02-05T12:00:00Z")
        self.assertEqual(ts.isoformat(), "2026-02-05T12:00:00+00:00")
        self.assertEqual(tz, "UTC")

    def test_normalizes_offset_to_utc(self) -> None:
        ts, tz = parse_timestamp("2026-02-05T11:00:00-05:00")
        self.assertEqual(ts.isoformat(), "2026-02-05T16:00:00+00:00")
        self.assertEqual(tz, "UTC-05:00")

    def test_naive_assumes_utc(self) -> None:
        ts, tz = parse_timestamp("2026-02-05T11:00:00")
        self.assertEqual(ts.isoformat(), "2026-02-05T11:00:00+00:00")
        self.assertIsNone(tz)


if __name__ == "__main__":
    unittest.main()
