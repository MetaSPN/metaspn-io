# Changelog

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
