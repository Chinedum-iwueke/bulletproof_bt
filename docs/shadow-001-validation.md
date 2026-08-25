# SHADOW-001 Validation

SHADOW-001 extends the existing Bulletproof execution runtime with a prospective, digest-bound journal and deterministic no-capital replay.

## Implemented

- Candidate, dataset, strategy, cost-model, and repository-commit bindings are fixed before the first observation.
- Decisions, order intents, simulated fills, heartbeats, reconciliations, and incidents are appended to a hash chain with per-record fsync.
- Stable event ids suppress duplicate delivery.
- Unsealed journals resume after interruption; sealed journals reject further writes.
- Replay fails closed on payload tampering, sequence drift, broken digest links, duplicate ids, binding drift, naive timestamps, and records after seal.
- Replay reconstructs event counts and canonical fee, slippage, and spread totals.
- Normal execution artifacts, state checkpoints, incident handling, and reconciliation remain the source runtime; SHADOW-001 does not duplicate those engines.
- Both the journal header and replay report state that capital, live-order, and venue-mutation authority are absent.

## Verification

```bash
pytest -q tests/exec
python scripts/shadow001_pilot.py --output /tmp/shadow001-pilot
python scripts/replay_exec_shadow.py \
  /tmp/shadow001-pilot/prospective_journal.jsonl
```

The pilot uses the repository's explicitly non-alpha sample pipeline smoke hypothesis, a deterministic one-row market fixture, a forced disconnect/restart, a duplicate event, reconciliation, journal sealing, replay, and a tamper attempt. It does not touch an exchange, credentials, capital, or production resources.

## Rollback

Disable or omit `shadow_journal.enabled`; the existing shadow runtime and its prior artifacts continue unchanged. Journal files are additive evidence and do not mutate SQLite execution state.
