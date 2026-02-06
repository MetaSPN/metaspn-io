from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def _parse_date_window(day: str | None) -> tuple[datetime | None, datetime | None]:
    if day is None:
        return None, None
    start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1) - timedelta(microseconds=1)
    return start, end


def _resolve_output_path(out: Path | None, day: str | None) -> Path | None:
    if out is None:
        return None
    if day is None:
        return out
    if out.suffix == ".jsonl":
        return out
    return out / f"{day}.jsonl"


def run_ingest(
    registry: AdapterRegistry,
    adapter_name: str,
    source: Path,
    out: Path | None = None,
    store: Path | None = None,
    day: str | None = None,
    since: str | None = None,
    until: str | None = None,
    dry_run: bool = False,
    stats: bool = False,
    lenient: bool = False,
    error_log_path: Path | None = None,
) -> IngestResult:
    adapter = registry.get(adapter_name)
    date_since, date_until = _parse_date_window(day)
    parsed_since = _parse_range(since)
    parsed_until = _parse_range(until)
    resolved_out = _resolve_output_path(out, day)

    options = AdapterOptions(
        since=parsed_since or date_since,
        until=parsed_until or date_until,
        lenient=lenient,
    )

    signals = [sig.to_dict() for sig in adapter.iter_signals(source, options=options)]
    issues = [issue.to_dict() for issue in getattr(adapter, "issues", [])]

    error_log = error_log_path
    if error_log is None and issues:
        error_log = Path("workspace/logs/ingest_errors.jsonl")

    if not dry_run:
        if resolved_out is not None:
            write_jsonl(resolved_out, signals)
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
        output=resolved_out,
        error_log=error_log,
    )
