# Changelog

## v0.1.4 - 2026-02-07
- Added `season1_onchain_jsonl_v1` adapter for Season 1 event ingestion: `season_init`, `game_create`, `distribute`, `stake`, `end`, and `claim`.
- Extended token adapter coverage with Season 1 attention inputs: `$METATOWEL` volume windows and reward pool funding events.
- Added deterministic sort/order/replay and strict-mode parse-issue tests for Season 1 ingestion.
- Added daily Season 1 CLI ingestion examples in README.

## v0.1.3 - 2026-02-06
- Added token signal adapters `solana_rpc_v1` and `pumpfun_v1` (experimental).
- Added canonical token event mappings: `trade`, `holder_change`, `supply_change`, `liquidity_event`, `metadata_update`, `reward_update`.
- Added fixture-based adapter tests for token mappings and envelope-level idempotent re-ingestion.
- Expanded default adapter registry to include token adapters.

## v0.1.2 - 2026-02-06
- Production hardening for test and release workflows.
- Standardized canonical test command to `python3 -m pytest -q`.
- Added CI workflow to run tests and build+artifact validation.
- Added smoke tests for adapter registry and CLI ingest command.
- Removed `PYTHONPATH=src` requirement for local pytest runs via test bootstrap.
