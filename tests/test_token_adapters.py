from __future__ import annotations

from pathlib import Path

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.adapters.pumpfun_jsonl import PumpfunAdapter
from metaspn_io.adapters.solana_rpc_jsonl import SolanaRpcAdapter

FIXTURES = Path(__file__).parent / "fixtures" / "tokens"


def test_solana_rpc_adapter_maps_all_token_event_types() -> None:
    adapter = SolanaRpcAdapter()
    signals = list(adapter.iter_signals(FIXTURES / "solana_rpc.jsonl", options=AdapterOptions()))

    assert len(signals) == 6
    assert [s.payload_type for s in signals] == [
        "TokenTradeSeen",
        "HolderChangeSeen",
        "SupplyChangeSeen",
        "LiquidityEventSeen",
        "TokenMetadataUpdated",
        "RewardUpdated",
    ]
    assert all(s.source == "solana" for s in signals)


def test_pumpfun_adapter_sets_source_and_is_idempotent() -> None:
    adapter_a = PumpfunAdapter()
    adapter_b = PumpfunAdapter()

    first = [s.to_dict() for s in adapter_a.iter_signals(FIXTURES / "pumpfun.jsonl", options=AdapterOptions())]
    second = [s.to_dict() for s in adapter_b.iter_signals(FIXTURES / "pumpfun.jsonl", options=AdapterOptions())]

    assert first == second
    assert len(first) == 2
    assert all(s["source"] == "pumpfun" for s in first)
    assert first[0]["payload_type"] == "TokenTradeSeen"
