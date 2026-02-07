from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.ids import stable_signal_id
from metaspn_io.io_utils import ParseIssue, iter_jsonl_records
from metaspn_io.models import (
    SCHEMA_VERSION,
    EntityRef,
    SeasonEnded,
    SeasonGameCreated,
    SeasonInitialized,
    SeasonRewardClaimed,
    SeasonRewardDistributed,
    SeasonStakeRecorded,
    SignalEnvelope,
    TraceContext,
    payload_type_name,
    utc_iso,
)
from metaspn_io.timeutils import TimestampError, in_range, parse_timestamp


@dataclass
class Season1OnchainJsonlAdapter:
    name: str = "season1_onchain_jsonl_v1"
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
                self.issues.append(ParseIssue(str(exc), row.input_file, row.input_line_number, repr(row.data)))
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
        event_type = str(data.get("type", "")).strip().lower()
        chain = str(data.get("chain", "solana")).strip().lower() or "solana"
        season_id = str(data.get("season_id", "")).strip()
        game_id = str(data.get("game_id", "")).strip()
        wallet = str(data.get("wallet", "")).strip()

        missing: list[str] = []
        for field, value in (("type", event_type), ("season_id", season_id)):
            if not value:
                missing.append(field)
        if missing and not options.lenient:
            raise ValueError(f"missing required fields: {', '.join(missing)}")

        raw_ts = data.get("timestamp")
        try:
            ts, original_tz = parse_timestamp(raw_ts) if raw_ts is not None else parse_timestamp("1970-01-01T00:00:00Z")
        except TimestampError as exc:
            if not options.lenient:
                raise ValueError(str(exc)) from exc
            ts, original_tz = parse_timestamp("1970-01-01T00:00:00Z")

        payload, key, identifier = self._map_payload(chain, event_type, season_id, game_id, wallet, data, options)
        signal = SignalEnvelope(
            schema_version=SCHEMA_VERSION,
            signal_id=stable_signal_id(chain, ts, key),
            timestamp=utc_iso(ts),
            source=chain,
            payload_type=payload_type_name(payload),
            payload=payload,
            entity_refs=[EntityRef(kind="platform_identifier", platform=chain, identifier=identifier)],
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

    def _map_payload(
        self,
        chain: str,
        event_type: str,
        season_id: str,
        game_id: str,
        wallet: str,
        data: dict[str, Any],
        options: AdapterOptions,
    ) -> tuple[Any, str, str]:
        if event_type == "season_init":
            payload = SeasonInitialized(chain=chain, season_id=season_id or "unknown", game_id=game_id or None)
            key = f"season_init|{payload.season_id}|{payload.game_id or ''}"
            identifier = payload.season_id
        elif event_type == "game_create":
            payload = SeasonGameCreated(
                chain=chain,
                season_id=season_id or "unknown",
                game_id=game_id or "unknown",
                creator=wallet or "unknown",
            )
            key = f"game_create|{payload.season_id}|{payload.game_id}|{payload.creator}"
            identifier = payload.game_id
        elif event_type == "distribute":
            payload = SeasonRewardDistributed(
                chain=chain,
                season_id=season_id or "unknown",
                game_id=game_id or "unknown",
                pool=str(data.get("pool", "unknown")),
                amount=float(data.get("amount", 0.0)),
            )
            key = (
                f"distribute|{payload.season_id}|{payload.game_id}|{payload.pool}|{payload.amount:.8f}"
            )
            identifier = payload.game_id
        elif event_type == "stake":
            payload = SeasonStakeRecorded(
                chain=chain,
                season_id=season_id or "unknown",
                game_id=game_id or "unknown",
                wallet=wallet or "unknown",
                amount=float(data.get("amount", 0.0)),
            )
            key = f"stake|{payload.season_id}|{payload.game_id}|{payload.wallet}|{payload.amount:.8f}"
            identifier = payload.wallet
        elif event_type == "end":
            payload = SeasonEnded(
                chain=chain,
                season_id=season_id or "unknown",
                game_id=game_id or "unknown",
                status=str(data.get("status", "ended")),
            )
            key = f"end|{payload.season_id}|{payload.game_id}|{payload.status}"
            identifier = payload.game_id
        elif event_type == "claim":
            payload = SeasonRewardClaimed(
                chain=chain,
                season_id=season_id or "unknown",
                game_id=game_id or "unknown",
                wallet=wallet or "unknown",
                amount=float(data.get("amount", 0.0)),
            )
            key = f"claim|{payload.season_id}|{payload.game_id}|{payload.wallet}|{payload.amount:.8f}"
            identifier = payload.wallet
        else:
            if not options.lenient:
                raise ValueError(f"unsupported type: {event_type}")
            payload = SeasonEnded(
                chain=chain,
                season_id=season_id or "unknown",
                game_id=game_id or "unknown",
                status="unknown",
            )
            key = f"fallback|{season_id}|{game_id}|{event_type}|{wallet}"
            identifier = season_id or "unknown"
        return payload, key, identifier
