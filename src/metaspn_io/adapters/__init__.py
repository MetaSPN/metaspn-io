from metaspn_io.adapters.outcomes_jsonl import OutcomesJsonlAdapter
from metaspn_io.adapters.pumpfun_jsonl import PumpfunAdapter
from metaspn_io.adapters.registry import AdapterRegistry
from metaspn_io.adapters.season1_onchain_jsonl import Season1OnchainJsonlAdapter
from metaspn_io.adapters.social_jsonl import SocialJsonlAdapter
from metaspn_io.adapters.solana_rpc_jsonl import SolanaRpcAdapter


def default_registry() -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(SocialJsonlAdapter())
    registry.register(OutcomesJsonlAdapter())
    registry.register(SolanaRpcAdapter())
    registry.register(PumpfunAdapter())
    registry.register(Season1OnchainJsonlAdapter())
    return registry


__all__ = [
    "AdapterRegistry",
    "SocialJsonlAdapter",
    "OutcomesJsonlAdapter",
    "SolanaRpcAdapter",
    "PumpfunAdapter",
    "Season1OnchainJsonlAdapter",
    "default_registry",
]
