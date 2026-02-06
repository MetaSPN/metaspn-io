from datetime import datetime, timezone
import unittest

from metaspn_io.ids import stable_signal_id


class StableIdTests(unittest.TestCase):
    def test_stable_id_idempotent(self) -> None:
        ts = datetime(2026, 2, 5, 12, 0, 0, tzinfo=timezone.utc)
        first = stable_signal_id("twitter", ts, "twitter|post|https://x.com/a/1|seen")
        second = stable_signal_id("twitter", ts, "twitter|post|https://x.com/a/1|seen")
        self.assertEqual(first, second)

    def test_stable_id_changes_for_different_keys(self) -> None:
        ts = datetime(2026, 2, 5, 12, 0, 0, tzinfo=timezone.utc)
        a = stable_signal_id("twitter", ts, "key-a")
        b = stable_signal_id("twitter", ts, "key-b")
        self.assertNotEqual(a, b)


if __name__ == "__main__":
    unittest.main()
