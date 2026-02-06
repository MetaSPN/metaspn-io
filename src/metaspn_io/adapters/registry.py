from __future__ import annotations

from dataclasses import dataclass, field

from metaspn_io.adapters.base import Adapter


@dataclass
class AdapterRegistry:
    _adapters: dict[str, Adapter] = field(default_factory=dict)

    def register(self, adapter: Adapter) -> None:
        self._adapters[adapter.name] = adapter

    def get(self, name: str) -> Adapter:
        try:
            return self._adapters[name]
        except KeyError as exc:
            known = ", ".join(sorted(self._adapters))
            raise KeyError(f"Unknown adapter '{name}'. Known adapters: {known}") from exc

    def names(self) -> list[str]:
        return sorted(self._adapters)
