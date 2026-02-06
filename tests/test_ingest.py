from pathlib import Path
import tempfile
import unittest

from metaspn_io.adapters import default_registry
from metaspn_io.ingest import run_ingest

FIXTURES = Path(__file__).parent / "fixtures" / "social"


class IngestTests(unittest.TestCase):
    def test_ingest_writes_out_and_errors(self) -> None:
        registry = default_registry()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "signals.jsonl"
            errors = Path(tmpdir) / "ingest_errors.jsonl"

            result = run_ingest(
                registry=registry,
                adapter_name="social_jsonl_v1",
                source=FIXTURES,
                out=out,
                error_log_path=errors,
            )

            self.assertEqual(result.emitted, 4)
            self.assertEqual(result.errors, 2)
            self.assertTrue(out.exists())
            self.assertTrue(errors.exists())


if __name__ == "__main__":
    unittest.main()
