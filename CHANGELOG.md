# Changelog

## v0.1.2 - 2026-02-06
- Production hardening for test and release workflows.
- Standardized canonical test command to `python3 -m pytest -q`.
- Added CI workflow to run tests and build+artifact validation.
- Added smoke tests for adapter registry and CLI ingest command.
- Removed `PYTHONPATH=src` requirement for local pytest runs via test bootstrap.
