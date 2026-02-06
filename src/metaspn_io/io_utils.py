from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RawRecord:
    data: dict[str, Any]
    input_file: str
    input_line_number: int


@dataclass(frozen=True)
class ParseIssue:
    message: str
    input_file: str
    input_line_number: int
    raw_line: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "input_file": self.input_file,
            "input_line_number": self.input_line_number,
            "raw_line": self.raw_line,
        }


def iter_jsonl_paths(source_path: Path) -> Iterator[Path]:
    if source_path.is_file():
        yield source_path
        return

    for path in sorted(source_path.glob("*.jsonl")):
        if path.is_file():
            yield path


def iter_jsonl_records(source_path: Path) -> Iterator[RawRecord | ParseIssue]:
    for path in iter_jsonl_paths(source_path):
        with path.open("r", encoding="utf-8") as f:
            for idx, line in enumerate(f, start=1):
                raw_line = line.rstrip("\n")
                if not raw_line.strip():
                    continue
                try:
                    parsed = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    yield ParseIssue(
                        message=f"invalid json: {exc}",
                        input_file=str(path),
                        input_line_number=idx,
                        raw_line=raw_line,
                    )
                    continue
                if not isinstance(parsed, dict):
                    yield ParseIssue(
                        message="json line must be an object",
                        input_file=str(path),
                        input_line_number=idx,
                        raw_line=raw_line,
                    )
                    continue
                yield RawRecord(data=parsed, input_file=str(path), input_line_number=idx)


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":"), sort_keys=True))
            f.write("\n")


def append_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":"), sort_keys=True))
            f.write("\n")
