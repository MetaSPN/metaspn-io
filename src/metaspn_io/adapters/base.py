from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from metaspn_io.models import SignalEnvelope


@dataclass(frozen=True)
class AdapterOptions:
    since: object | None = None
    until: object | None = None
    lenient: bool = False


class Adapter(Protocol):
    name: str
    version: str

    def iter_signals(self, source_path: Path, options: AdapterOptions | None = None) -> Iterator[SignalEnvelope]:
        ...
