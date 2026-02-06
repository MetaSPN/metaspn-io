from metaspn_io.adapters.outcomes_jsonl import OutcomesJsonlAdapter
from metaspn_io.adapters.registry import AdapterRegistry
from metaspn_io.adapters.social_jsonl import SocialJsonlAdapter


def default_registry() -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(SocialJsonlAdapter())
    registry.register(OutcomesJsonlAdapter())
    return registry


__all__ = [
    "AdapterRegistry",
    "SocialJsonlAdapter",
    "OutcomesJsonlAdapter",
    "default_registry",
]
