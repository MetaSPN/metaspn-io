from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from typing import Any

SCHEMA_VERSION = "0.1"


@dataclass(frozen=True)
class EntityRef:
    kind: str
    platform: str
    identifier: str


@dataclass(frozen=True)
class TraceContext:
    ingested_at: str
    input_file: str
    input_line_number: int
    adapter_name: str
    adapter_version: str
    raw_id: str | None = None
    original_timezone: str | None = None


try:
    from metaspn_schemas import (  # type: ignore[attr-defined]
        MeetingBooked,
        MessageSent,
        ProfileSnapshotSeen,
        ReplyReceived,
        RevenueEvent,
        SocialPostSeen,
    )
except Exception:
    @dataclass(frozen=True)
    class SocialPostSeen:
        platform: str
        author_handle: str
        post_url: str
        text: str
        action: str = "seen"


    @dataclass(frozen=True)
    class ProfileSnapshotSeen:
        platform: str
        author_handle: str
        profile_url: str
        text: str | None = None


    @dataclass(frozen=True)
    class MessageSent:
        channel: str
        recipient: str
        subject: str | None = None


    @dataclass(frozen=True)
    class ReplyReceived:
        channel: str
        sender: str
        subject: str | None = None


    @dataclass(frozen=True)
    class MeetingBooked:
        participant: str
        meeting_id: str | None = None


    @dataclass(frozen=True)
    class RevenueEvent:
        account: str
        amount: float
        currency: str = "USD"

try:
    from metaspn_schemas import (  # type: ignore[attr-defined]
        HolderChangeSeen,
        LiquidityEventSeen,
        RewardUpdated,
        SupplyChangeSeen,
        TokenMetadataUpdated,
        TokenTradeSeen,
    )
except Exception:
    @dataclass(frozen=True)
    class TokenTradeSeen:
        chain: str
        token_mint: str
        wallet: str
        side: str
        amount: float
        price_usd: float | None = None


    @dataclass(frozen=True)
    class HolderChangeSeen:
        chain: str
        token_mint: str
        wallet: str
        delta: float


    @dataclass(frozen=True)
    class SupplyChangeSeen:
        chain: str
        token_mint: str
        new_supply: float
        delta: float | None = None


    @dataclass(frozen=True)
    class LiquidityEventSeen:
        chain: str
        token_mint: str
        pool: str
        action: str
        amount: float


    @dataclass(frozen=True)
    class TokenMetadataUpdated:
        chain: str
        token_mint: str
        field: str
        value: str


    @dataclass(frozen=True)
    class RewardUpdated:
        chain: str
        token_mint: str
        wallet: str
        program: str
        amount: float


@dataclass(frozen=True)
class SignalEnvelope:
    schema_version: str
    signal_id: str
    timestamp: str
    source: str
    payload_type: str
    payload: Any
    entity_refs: list[EntityRef]
    trace: TraceContext

    def to_dict(self) -> dict[str, Any]:
        payload_obj = self.payload
        if is_dataclass(payload_obj):
            payload_obj = asdict(payload_obj)

        return {
            "schema_version": self.schema_version,
            "signal_id": self.signal_id,
            "timestamp": self.timestamp,
            "source": self.source,
            "payload_type": self.payload_type,
            "payload": payload_obj,
            "entity_refs": [asdict(ref) for ref in self.entity_refs],
            "trace": asdict(self.trace),
        }


PAYLOAD_TYPES: dict[str, type] = {
    "SocialPostSeen": SocialPostSeen,
    "ProfileSnapshotSeen": ProfileSnapshotSeen,
    "MessageSent": MessageSent,
    "ReplyReceived": ReplyReceived,
    "MeetingBooked": MeetingBooked,
    "RevenueEvent": RevenueEvent,
    "TokenTradeSeen": TokenTradeSeen,
    "HolderChangeSeen": HolderChangeSeen,
    "SupplyChangeSeen": SupplyChangeSeen,
    "LiquidityEventSeen": LiquidityEventSeen,
    "TokenMetadataUpdated": TokenMetadataUpdated,
    "RewardUpdated": RewardUpdated,
}


def payload_type_name(payload: Any) -> str:
    return type(payload).__name__


def utc_iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
