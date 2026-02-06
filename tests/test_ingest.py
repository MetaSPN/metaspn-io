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

    def test_date_window_writes_partition_and_is_rerun_stable(self) -> None:
        registry = default_registry()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "signals"
            errors = Path(tmpdir) / "ingest_errors.jsonl"
            first = run_ingest(
                registry=registry,
                adapter_name="social_jsonl_v1",
                source=FIXTURES,
                day="2026-02-05",
                out=out_dir,
                error_log_path=errors,
            )

            expected_out = out_dir / "2026-02-05.jsonl"
            self.assertEqual(first.output, expected_out)
            self.assertTrue(expected_out.exists())
            content_a = expected_out.read_text(encoding="utf-8")

            second = run_ingest(
                registry=registry,
                adapter_name="social_jsonl_v1",
                source=FIXTURES,
                day="2026-02-05",
                out=out_dir,
                error_log_path=errors,
            )
            self.assertEqual(second.output, expected_out)
            content_b = expected_out.read_text(encoding="utf-8")

            self.assertEqual(content_a, content_b)
            self.assertEqual(first.emitted, 3)
            self.assertEqual(second.emitted, 3)

    def test_lenient_mode_recovers_bad_timestamp_and_type(self) -> None:
        registry = default_registry()
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "signals.jsonl"
            errors = Path(tmpdir) / "errors.jsonl"
            result = run_ingest(
                registry=registry,
                adapter_name="social_jsonl_v1",
                source=FIXTURES,
                out=out,
                lenient=True,
                error_log_path=errors,
            )

            self.assertEqual(result.emitted, 6)
            self.assertEqual(result.errors, 0)
            self.assertFalse(errors.exists())

    def test_strict_mode_logs_malformed_json_line(self) -> None:
        registry = default_registry()
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "broken.jsonl"
            source.write_text(
                "{\"platform\":\"twitter\",\"type\":\"post_seen\",\"author_handle\":\"ok\",\"url\":\"https://x.com/ok/status/1\",\"timestamp\":\"2026-02-05T12:00:00Z\"}\n"
                "not json\n"
                "[1,2,3]\n",
                encoding="utf-8",
            )
            out = Path(tmpdir) / "signals.jsonl"
            errors = Path(tmpdir) / "errors.jsonl"
            result = run_ingest(
                registry=registry,
                adapter_name="social_jsonl_v1",
                source=source,
                out=out,
                error_log_path=errors,
            )

            self.assertEqual(result.emitted, 1)
            self.assertEqual(result.errors, 2)
            self.assertTrue(errors.exists())


if __name__ == "__main__":
    unittest.main()
