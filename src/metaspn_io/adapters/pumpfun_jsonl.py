from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.adapters.solana_rpc_jsonl import SolanaRpcAdapter


@dataclass
class PumpfunAdapter(SolanaRpcAdapter):
    name: str = "pumpfun_v1"
    version: str = "0.1-exp"

    def iter_signals(self, source_path: Path, options: AdapterOptions | None = None):
        for signal in super().iter_signals(source_path, options=options):
            yield signal

    def _parse_record(
        self,
        data: dict[str, Any],
        input_file: str,
        line_number: int,
        options: AdapterOptions,
    ):
        payload = dict(data)
        payload.setdefault("chain", "pumpfun")
        return super()._parse_record(payload, input_file, line_number, options)
