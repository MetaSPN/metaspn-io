from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from metaspn_io.adapters.base import AdapterOptions
from metaspn_io.ids import stable_signal_id
from metaspn_io.io_utils import ParseIssue, iter_jsonl_records
from metaspn_io.models import (
    HolderChangeSeen,
    LiquidityEventSeen,
    RewardUpdated,
    SCHEMA_VERSION,
    SupplyChangeSeen,
    TokenMetadataUpdated,
    TokenTradeSeen,
    EntityRef,
    SignalEnvelope,
    TraceContext,
    payload_type_name,
    utc_iso,
)
from metaspn_io.timeutils import TimestampError, in_range, parse_timestamp


@dataclass
class SolanaRpcAdapter:
    name: str = "solana_rpc_v1"
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
        token_mint = str(data.get("token_mint", "")).strip()
        wallet = str(data.get("wallet", "")).strip()

        if not token_mint and not options.lenient:
            raise ValueError("missing required field: token_mint")

        raw_ts = data.get("timestamp")
        try:
            ts, original_tz = parse_timestamp(raw_ts) if raw_ts is not None else parse_timestamp(datetime.now(timezone.utc))
        except TimestampError as exc:
            if not options.lenient:
                raise ValueError(str(exc)) from exc
            ts, original_tz = parse_timestamp(datetime.now(timezone.utc))
        ingested_at, _ = parse_timestamp(datetime.now(timezone.utc))

        payload, key = self._map_payload(chain, event_type, token_mint, wallet, data, options)

        signal = SignalEnvelope(
            schema_version=SCHEMA_VERSION,
            signal_id=stable_signal_id(chain, ts, key),
            timestamp=utc_iso(ts),
            source=chain,
            payload_type=payload_type_name(payload),
            payload=payload,
            entity_refs=[EntityRef(kind="platform_identifier", platform=chain, identifier=token_mint or "unknown")],
            trace=TraceContext(
                ingested_at=utc_iso(ingested_at),
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
        token_mint: str,
        wallet: str,
        data: dict[str, Any],
        options: AdapterOptions,
    ) -> tuple[Any, str]:
        if event_type == "trade":
            payload = TokenTradeSeen(
                chain=chain,
                token_mint=token_mint or "unknown",
                wallet=wallet or "unknown",
                side=str(data.get("side", "unknown")),
                amount=float(data.get("amount", 0.0)),
                price_usd=None if data.get("price_usd") is None else float(data.get("price_usd")),
            )
            key = f"trade|{payload.token_mint}|{payload.wallet}|{payload.side}|{payload.amount:.8f}|{payload.price_usd}"
        elif event_type == "holder_change":
            payload = HolderChangeSeen(
                chain=chain,
                token_mint=token_mint or "unknown",
                wallet=wallet or "unknown",
                delta=float(data.get("delta", 0.0)),
            )
            key = f"holder_change|{payload.token_mint}|{payload.wallet}|{payload.delta:.8f}"
        elif event_type == "supply_change":
            payload = SupplyChangeSeen(
                chain=chain,
                token_mint=token_mint or "unknown",
                new_supply=float(data.get("new_supply", 0.0)),
                delta=None if data.get("delta") is None else float(data.get("delta")),
            )
            key = f"supply_change|{payload.token_mint}|{payload.new_supply:.8f}|{payload.delta}"
        elif event_type == "liquidity_event":
            payload = LiquidityEventSeen(
                chain=chain,
                token_mint=token_mint or "unknown",
                pool=str(data.get("pool", "unknown")),
                action=str(data.get("action", "unknown")),
                amount=float(data.get("amount", 0.0)),
            )
            key = f"liquidity_event|{payload.token_mint}|{payload.pool}|{payload.action}|{payload.amount:.8f}"
        elif event_type == "metadata_update":
            payload = TokenMetadataUpdated(
                chain=chain,
                token_mint=token_mint or "unknown",
                field=str(data.get("field", "unknown")),
                value=str(data.get("value", "")),
            )
            key = f"metadata_update|{payload.token_mint}|{payload.field}|{payload.value}"
        elif event_type == "reward_update":
            payload = RewardUpdated(
                chain=chain,
                token_mint=token_mint or "unknown",
                wallet=wallet or "unknown",
                program=str(data.get("program", "unknown")),
                amount=float(data.get("amount", 0.0)),
            )
            key = f"reward_update|{payload.token_mint}|{payload.wallet}|{payload.program}|{payload.amount:.8f}"
        else:
            if not options.lenient:
                raise ValueError(f"unsupported type: {event_type}")
            payload = TokenMetadataUpdated(
                chain=chain,
                token_mint=token_mint or "unknown",
                field="unknown",
                value=str(data),
            )
            key = f"fallback|{token_mint}|{event_type}|{wallet}"
        return payload, key
