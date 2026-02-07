from __future__ import annotations

from pathlib import Path
import tempfile

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.adapters.season1_onchain_jsonl import Season1OnchainJsonlAdapter
from metaspn_io.adapters import default_registry
from metaspn_io.ingest import run_ingest

FIXTURES = Path(__file__).parent / "fixtures" / "season1"


def test_season1_adapter_maps_all_supported_event_types() -> None:
    adapter = Season1OnchainJsonlAdapter()
    signals = list(adapter.iter_signals(FIXTURES / "onchain.jsonl", options=AdapterOptions()))

    assert len(signals) == 6
    assert [s.payload_type for s in signals] == [
        "SeasonInitialized",
        "SeasonGameCreated",
        "SeasonRewardDistributed",
        "SeasonStakeRecorded",
        "SeasonEnded",
        "SeasonRewardClaimed",
    ]
    assert [s.timestamp for s in signals] == sorted(s.timestamp for s in signals)
    assert all(s.timestamp.endswith("Z") for s in signals)
    assert all(s.source == "solana" for s in signals)
    assert len(adapter.issues) == 1


def test_season1_adapter_replay_is_byte_equivalent() -> None:
    registry = default_registry()
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "signals.jsonl"
        errors = Path(tmpdir) / "errors.jsonl"

        first = run_ingest(
            registry=registry,
            adapter_name="season1_onchain_jsonl_v1",
            source=FIXTURES / "onchain.jsonl",
            out=out,
            error_log_path=errors,
        )
        content_a = out.read_text(encoding="utf-8")

        second = run_ingest(
            registry=registry,
            adapter_name="season1_onchain_jsonl_v1",
            source=FIXTURES / "onchain.jsonl",
            out=out,
            error_log_path=errors,
        )
        content_b = out.read_text(encoding="utf-8")

        assert first.emitted == second.emitted == 6
        assert first.errors == second.errors == 1
        assert content_a == content_b
