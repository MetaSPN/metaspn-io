# metaspn-io

`metaspn-io` is the ingestion and normalization layer for MetaSPN. It converts raw external records into canonical signal envelopes with deterministic IDs and ordering.

## Quick Demo (5 lines)
```bash
python -m metaspn_io io ingest \
  --adapter social_jsonl_v1 \
  --source tests/fixtures/social \
  --date 2026-02-05 \
  --out /tmp/social-signals \
  --stats
```

## v0.1 Adapters
- `social_jsonl_v1` (MUST): browser-extension social JSONL (`post_seen`, `profile_seen`)
- `outcomes_jsonl_v1` (SHOULD): manual outcomes JSONL (`message_sent`, `reply_received`, `meeting_booked`, `revenue_event`)

## Schema Mapping
| Input adapter | Input `type` | Output payload |
|---|---|---|
| `social_jsonl_v1` | `post_seen` | `SocialPostSeen` |
| `social_jsonl_v1` | `profile_seen` | `ProfileSnapshotSeen` |
| `outcomes_jsonl_v1` | `message_sent` | `MessageSent` |
| `outcomes_jsonl_v1` | `reply_received` | `ReplyReceived` |
| `outcomes_jsonl_v1` | `meeting_booked` | `MeetingBooked` |
| `outcomes_jsonl_v1` | `revenue_event` | `RevenueEvent` |

## Output Envelope
JSONL lines are emitted as canonical envelopes:
```json
{
  "schema_version": "0.1",
  "signal_id": "s_4e9b5c8417d3af2ef9baf8d1",
  "timestamp": "2026-02-05T12:00:00Z",
  "source": "twitter",
  "payload_type": "SocialPostSeen",
  "payload": {
    "platform": "twitter",
    "author_handle": "alice",
    "post_url": "https://x.com/alice/status/1",
    "text": "hello world",
    "action": "seen"
  },
  "entity_refs": [
    {
      "kind": "platform_identifier",
      "platform": "twitter",
      "identifier": "alice"
    }
  ],
  "trace": {
    "ingested_at": "2026-02-06T00:00:00Z",
    "input_file": "raw/social/2026-02-05.jsonl",
    "input_line_number": 1,
    "adapter_name": "social_jsonl_v1",
    "adapter_version": "0.1",
    "raw_id": null,
    "original_timezone": "UTC"
  }
}
```

## CLI
Primary command:
```bash
metaspn io ingest --adapter social_jsonl_v1 --source raw/social --out workspace/store/signals/2026-02-05.jsonl
```

Supported flags:
- `--source` file or directory
- `--out` output JSONL path or directory (with `--date`, writes `<out>/<date>.jsonl`)
- `--store` optional store root (writes to `<store>/signals/YYYY-MM-DD.jsonl`)
- `--date` one-day UTC ingest window (`YYYY-MM-DD`)
- `--since` ISO timestamp lower bound
- `--until` ISO timestamp upper bound
- `--dry-run`
- `--stats`
- `--lenient`

Demo orchestrator invocation:
```bash
metaspn io ingest --adapter social_jsonl_v1 --source raw/social --date 2026-02-05 --out workspace/store/signals
```

Default mode is strict: bad records are skipped and logged to `workspace/logs/ingest_errors.jsonl` unless overridden.

## Determinism Rules
- Stable IDs via `stable_signal_id(source, timestamp, key)`
- Timestamps normalized to UTC
- Deterministic sort: timestamp, then canonical key
- JSON output uses sorted keys

## Add A New Adapter (<50 lines)
```python
from dataclasses import dataclass
from pathlib import Path
from metaspn_io.adapters.base import AdapterOptions

@dataclass
class MyAdapter:
    name: str = "my_adapter_v1"
    version: str = "0.1"

    def iter_signals(self, source_path: Path, options: AdapterOptions | None = None):
        for raw in iter_jsonl_records(source_path):
            if isinstance(raw, ParseIssue):
                self.issues.append(raw)
                continue
            signal = convert_to_signal(raw)
            yield signal
```

Register it in `metaspn_io.adapters.default_registry()`.

## Tests
```bash
python3 -m pytest -q
```

## Publishing
`publish.yml` publishes to PyPI when you push a version tag:
```bash
git tag -a v0.1.2 -m "v0.1.2"
git push origin v0.1.2
```
Configure PyPI trusted publishing for this GitHub repository, then the workflow will upload `dist/*` automatically.
Before tagging, ensure CI (`.github/workflows/ci.yml`) is green, which validates `python3 -m pytest -q` and package build artifacts.
