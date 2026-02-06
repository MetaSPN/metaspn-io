from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.adapters.registry import AdapterRegistry
from metaspn_io.io_utils import append_jsonl, write_jsonl
from metaspn_io.timeutils import parse_timestamp


@dataclass(frozen=True)
class IngestResult:
    emitted: int
    errors: int
    output: Path | None
    error_log: Path | None


def _parse_range(value: str | None) -> datetime | None:
    if value is None:
        return None
    ts, _ = parse_timestamp(value)
    return ts


def run_ingest(
    registry: AdapterRegistry,
    adapter_name: str,
    source: Path,
    out: Path | None = None,
    store: Path | None = None,
    since: str | None = None,
    until: str | None = None,
    dry_run: bool = False,
    stats: bool = False,
    lenient: bool = False,
    error_log_path: Path | None = None,
) -> IngestResult:
    adapter = registry.get(adapter_name)

    options = AdapterOptions(
        since=_parse_range(since),
        until=_parse_range(until),
        lenient=lenient,
    )

    signals = [sig.to_dict() for sig in adapter.iter_signals(source, options=options)]
    issues = [issue.to_dict() for issue in getattr(adapter, "issues", [])]

    error_log = error_log_path
    if error_log is None and issues:
        error_log = Path("workspace/logs/ingest_errors.jsonl")

    if not dry_run:
        if out is not None:
            write_jsonl(out, signals)
        if store is not None:
            for signal in signals:
                day = str(signal["timestamp"])[:10]
                partition = store / "signals" / f"{day}.jsonl"
                append_jsonl(partition, [signal])
        if issues and error_log is not None:
            append_jsonl(error_log, issues)

    if stats:
        by_payload: dict[str, int] = {}
        for sig in signals:
            payload_type = str(sig["payload_type"])
            by_payload[payload_type] = by_payload.get(payload_type, 0) + 1
        print(f"adapter={adapter_name}")
        print(f"source={source}")
        print(f"emitted={len(signals)}")
        print(f"errors={len(issues)}")
        for payload_type, count in sorted(by_payload.items()):
            print(f"payload.{payload_type}={count}")
        if error_log is not None:
            print(f"error_log={error_log}")

    return IngestResult(
        emitted=len(signals),
        errors=len(issues),
        output=out,
        error_log=error_log,
    )
