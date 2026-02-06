from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.ids import stable_signal_id
from metaspn_io.io_utils import ParseIssue, iter_jsonl_records
from metaspn_io.models import (
    SCHEMA_VERSION,
    EntityRef,
    ProfileSnapshotSeen,
    SignalEnvelope,
    SocialPostSeen,
    TraceContext,
    payload_type_name,
    utc_iso,
)
from metaspn_io.timeutils import TimestampError, in_range, parse_timestamp


@dataclass
class SocialJsonlAdapter:
    name: str = "social_jsonl_v1"
    version: str = "0.1"

    def __init__(self) -> None:
        self.issues: list[ParseIssue] = []

    def iter_signals(self, source_path: Path, options: AdapterOptions | None = None):
        opts = options or AdapterOptions()
        rows: list[tuple[datetime, str, SignalEnvelope]] = []
        self.issues = []

        for row in iter_jsonl_records(source_path):
            if isinstance(row, ParseIssue):
                self.issues.append(row)
                continue
            try:
                signal, ts, key = self._parse_record(row.data, row.input_file, row.input_line_number, opts)
            except ValueError as exc:
                self.issues.append(
                    ParseIssue(str(exc), row.input_file, row.input_line_number, repr(row.data))
                )
                continue
            if not in_range(ts, opts.since, opts.until):
                continue
            rows.append((ts, key, signal))

        rows.sort(key=lambda item: (item[0], item[1]))
        for _, _, signal in rows:
            yield signal

    def _parse_record(
        self,
        data: dict[str, Any],
        input_file: str,
        line_number: int,
        options: AdapterOptions,
    ) -> tuple[SignalEnvelope, datetime, str]:
        platform = str(data.get("platform", "")).strip().lower()
        typ = str(data.get("type", "")).strip().lower()
        author = str(data.get("author_handle", "")).strip()
        url = str(data.get("url", "")).strip()
        text = data.get("text")
        raw_ts = data.get("timestamp")

        missing: list[str] = []
        for field, val in (
            ("platform", platform),
            ("type", typ),
            ("author_handle", author),
            ("url", url),
            ("timestamp", raw_ts),
        ):
            if val in (None, ""):
                missing.append(field)
        if missing and not options.lenient:
            raise ValueError(f"missing required fields: {', '.join(missing)}")

        if not platform:
            platform = "unknown"

        try:
            ts, original_tz = parse_timestamp(raw_ts) if raw_ts is not None else parse_timestamp(datetime.now(timezone.utc))
        except TimestampError as exc:
            if not options.lenient:
                raise ValueError(str(exc)) from exc
            ts, original_tz = parse_timestamp(datetime.now(timezone.utc))

        ingested_at, _ = parse_timestamp(datetime.now(timezone.utc))

        if typ == "post_seen":
            payload = SocialPostSeen(
                platform=platform,
                author_handle=author or "unknown",
                post_url=url or "",
                text=str(text or ""),
                action="seen",
            )
            stable_key = f"{platform}|post|{payload.post_url}|seen"
        elif typ == "profile_seen":
            payload = ProfileSnapshotSeen(
                platform=platform,
                author_handle=author or "unknown",
                profile_url=url or "",
                text=None if text is None else str(text),
            )
            stable_key = f"{platform}|profile|{payload.profile_url}"
        else:
            if not options.lenient:
                raise ValueError(f"unsupported type: {typ}")
            payload = ProfileSnapshotSeen(
                platform=platform,
                author_handle=author or "unknown",
                profile_url=url or "",
                text=None if text is None else str(text),
            )
            stable_key = f"{platform}|fallback|{url}"

        signal_id = stable_signal_id(platform, ts, stable_key)
        trace = TraceContext(
            ingested_at=utc_iso(ingested_at),
            input_file=input_file,
            input_line_number=line_number,
            adapter_name=self.name,
            adapter_version=self.version,
            raw_id=None if data.get("raw_id") is None else str(data.get("raw_id")),
            original_timezone=original_tz,
        )

        signal = SignalEnvelope(
            schema_version=SCHEMA_VERSION,
            signal_id=signal_id,
            timestamp=utc_iso(ts),
            source=platform,
            payload_type=payload_type_name(payload),
            payload=payload,
            entity_refs=[
                EntityRef(
                    kind="platform_identifier",
                    platform=platform,
                    identifier=author or "unknown",
                )
            ],
            trace=trace,
        )
        return signal, ts, stable_key
