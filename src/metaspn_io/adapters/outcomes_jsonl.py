from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.ids import stable_signal_id
from metaspn_io.io_utils import ParseIssue, iter_jsonl_records
from metaspn_io.models import (
    MeetingBooked,
    MessageSent,
    ReplyReceived,
    RevenueEvent,
    SCHEMA_VERSION,
    EntityRef,
    SignalEnvelope,
    TraceContext,
    payload_type_name,
    utc_iso,
)
from metaspn_io.timeutils import TimestampError, in_range, parse_timestamp


@dataclass
class OutcomesJsonlAdapter:
    name: str = "outcomes_jsonl_v1"
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
        typ = str(data.get("type", "")).strip()
        source = str(data.get("source", "manual")).strip().lower() or "manual"
        actor = str(data.get("actor", "")).strip()

        raw_ts = data.get("timestamp")
        try:
            ts, original_tz = parse_timestamp(raw_ts) if raw_ts is not None else parse_timestamp("1970-01-01T00:00:00Z")
        except TimestampError as exc:
            if not options.lenient:
                raise ValueError(str(exc)) from exc
            ts, original_tz = parse_timestamp("1970-01-01T00:00:00Z")

        if typ == "message_sent":
            payload = MessageSent(channel=str(data.get("channel", "manual")), recipient=actor or "unknown", subject=data.get("subject"))
            key = f"{typ}|{payload.channel}|{payload.recipient}|{payload.subject or ''}"
            identifier = payload.recipient
        elif typ == "reply_received":
            payload = ReplyReceived(channel=str(data.get("channel", "manual")), sender=actor or "unknown", subject=data.get("subject"))
            key = f"{typ}|{payload.channel}|{payload.sender}|{payload.subject or ''}"
            identifier = payload.sender
        elif typ == "meeting_booked":
            payload = MeetingBooked(participant=actor or "unknown", meeting_id=data.get("meeting_id"))
            key = f"{typ}|{payload.participant}|{payload.meeting_id or ''}"
            identifier = payload.participant
        elif typ == "revenue_event":
            amount = float(data.get("amount", 0.0))
            payload = RevenueEvent(
                account=actor or "unknown",
                amount=amount,
                currency=str(data.get("currency", "USD")),
            )
            key = f"{typ}|{payload.account}|{payload.amount:.2f}|{payload.currency}"
            identifier = payload.account
        else:
            if not options.lenient:
                raise ValueError(f"unsupported type: {typ}")
            payload = MessageSent(channel="manual", recipient=actor or "unknown", subject=data.get("subject"))
            key = f"fallback|{actor}|{data.get('subject', '')}"
            identifier = payload.recipient

        signal = SignalEnvelope(
            schema_version=SCHEMA_VERSION,
            signal_id=stable_signal_id(source, ts, key),
            timestamp=utc_iso(ts),
            source=source,
            payload_type=payload_type_name(payload),
            payload=payload,
            entity_refs=[EntityRef(kind="platform_identifier", platform=source, identifier=identifier)],
            trace=TraceContext(
                ingested_at=utc_iso(ts),
                input_file=input_file,
                input_line_number=line_number,
                adapter_name=self.name,
                adapter_version=self.version,
                raw_id=None if data.get("raw_id") is None else str(data.get("raw_id")),
                original_timezone=original_tz,
            ),
        )
        return signal, ts, key
