from pathlib import Path
import unittest

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.adapters.outcomes_jsonl import OutcomesJsonlAdapter

FIXTURE = Path(__file__).parent / "fixtures" / "outcomes" / "outcomes.jsonl"


class OutcomesAdapterTests(unittest.TestCase):
    def test_parse_outcomes_fixture(self) -> None:
        adapter = OutcomesJsonlAdapter()
        signals = list(adapter.iter_signals(FIXTURE, options=AdapterOptions()))

        self.assertEqual(len(signals), 4)
        self.assertEqual([s.payload_type for s in signals], [
            "MessageSent",
            "ReplyReceived",
            "MeetingBooked",
            "RevenueEvent",
        ])


if __name__ == "__main__":
    unittest.main()
