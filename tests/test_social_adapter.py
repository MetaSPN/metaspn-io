from pathlib import Path
import unittest

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.adapters.social_jsonl import SocialJsonlAdapter

FIXTURES = Path(__file__).parent / "fixtures" / "social"


class SocialAdapterTests(unittest.TestCase):
    def test_parse_fixture_and_payload_types(self) -> None:
        adapter = SocialJsonlAdapter()
        signals = list(adapter.iter_signals(FIXTURES, options=AdapterOptions()))

        self.assertEqual(len(signals), 4)
        self.assertEqual(signals[0].payload_type, "SocialPostSeen")
        self.assertEqual(signals[1].payload_type, "SocialPostSeen")
        self.assertEqual(signals[2].payload_type, "ProfileSnapshotSeen")
        self.assertEqual(signals[-1].payload_type, "SocialPostSeen")

    def test_deterministic_ordering(self) -> None:
        adapter_a = SocialJsonlAdapter()
        adapter_b = SocialJsonlAdapter()

        first = [s.signal_id for s in adapter_a.iter_signals(FIXTURES, options=AdapterOptions())]
        second = [s.signal_id for s in adapter_b.iter_signals(FIXTURES, options=AdapterOptions())]

        self.assertEqual(first, second)

    def test_trace_metadata_fields_present(self) -> None:
        adapter = SocialJsonlAdapter()
        signal = next(iter(adapter.iter_signals(FIXTURES, options=AdapterOptions())))

        self.assertEqual(signal.trace.adapter_name, "social_jsonl_v1")
        self.assertEqual(signal.trace.adapter_version, "0.1")
        self.assertTrue(signal.trace.input_file.endswith(".jsonl"))
        self.assertGreaterEqual(signal.trace.input_line_number, 1)
        self.assertTrue(signal.trace.ingested_at.endswith("Z"))


if __name__ == "__main__":
    unittest.main()
