from __future__ import annotations

import argparse
from pathlib import Path

from metaspn_io.adapters import default_registry
from metaspn_io.ingest import run_ingest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="metaspn")
    subparsers = parser.add_subparsers(dest="command")

    io_parser = subparsers.add_parser("io")
    io_sub = io_parser.add_subparsers(dest="io_command")

    ingest = io_sub.add_parser("ingest", help="Ingest and normalize raw source data")
    ingest.add_argument("--adapter", required=True)
    ingest.add_argument("--source", required=True)
    ingest.add_argument("--out")
    ingest.add_argument("--store")
    ingest.add_argument("--date", help="UTC date window to ingest (YYYY-MM-DD)")
    ingest.add_argument("--since")
    ingest.add_argument("--until")
    ingest.add_argument("--dry-run", action="store_true")
    ingest.add_argument("--stats", action="store_true")
    ingest.add_argument("--lenient", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command != "io" or args.io_command != "ingest":
        parser.print_help()
        return 2

    if not args.out and not args.store and not args.dry_run:
        parser.error("at least one of --out, --store, or --dry-run is required")

    registry = default_registry()
    run_ingest(
        registry=registry,
        adapter_name=args.adapter,
        source=Path(args.source),
        out=Path(args.out) if args.out else None,
        store=Path(args.store) if args.store else None,
        day=args.date,
        since=args.since,
        until=args.until,
        dry_run=args.dry_run,
        stats=args.stats,
        lenient=args.lenient,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
