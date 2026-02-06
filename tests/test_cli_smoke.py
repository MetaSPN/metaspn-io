from __future__ import annotations

import tempfile
from pathlib import Path

from metaspn_io.adapters import default_registry
from metaspn_io.cli import main


FIXTURES = Path(__file__).parent / "fixtures" / "social"


def test_default_registry_contains_demo_adapters() -> None:
    registry = default_registry()
    assert registry.names() == [
        "outcomes_jsonl_v1",
        "pumpfun_v1",
        "social_jsonl_v1",
        "solana_rpc_v1",
    ]


def test_cli_ingest_smoke_date_partition() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        out_dir = Path(tmpdir) / "signals"
        exit_code = main(
            [
                "io",
                "ingest",
                "--adapter",
                "social_jsonl_v1",
                "--source",
                str(FIXTURES),
                "--date",
                "2026-02-05",
                "--out",
                str(out_dir),
            ]
        )
        assert exit_code == 0
        out_file = out_dir / "2026-02-05.jsonl"
        assert out_file.exists()
        lines = out_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3
