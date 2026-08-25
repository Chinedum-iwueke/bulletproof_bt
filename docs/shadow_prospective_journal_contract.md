# SHADOW-001 Prospective Journal and Replay Contract

## Purpose

SHADOW-001 records decisions and simulated execution forward in time without capital or venue-order authority, then deterministically verifies the same immutable observation stream after the session. It extends the existing Bulletproof execution runtime; it does not create another strategy or fill engine.

## Admission boundary

Before the first observation, the run must pin lowercase digests for the admitted candidate, immutable dataset snapshot, strategy implementation, execution cost model, and the exact repository commit. A missing or malformed binding prevents startup. The first journal record permanently states that capital, live orders, and venue mutation are prohibited.

Configure an admitted shadow run under `shadow_journal`:

```yaml
shadow_journal:
  enabled: true
  bindings:
    candidate_digest: <sha256>
    dataset_digest: <sha256>
    strategy_digest: <sha256>
    cost_model_digest: <sha256>
    source_commit: <git-sha1>
```

## Journal

`prospective_journal.jsonl` is append-only and fsyncs every accepted record. Each record includes a monotonic sequence, stable event id, timezone-aware observation time, canonical payload digest, previous-record digest, and record digest. Repeated event ids are ignored. Digest-chain, sequence, payload, authority, binding, and terminal-seal violations fail replay.

The existing runtime sends decisions, order intents, simulated fills, heartbeats, reconciliation results, and incidents into this journal while retaining its normal artifacts and SQLite recovery checkpoints.

## Replay

Run:

```bash
python scripts/replay_exec_shadow.py \
  outputs/exec_runs/<run-id>/prospective_journal.jsonl
```

Replay writes `shadow_replay_report.json`, reproduces event counts and execution cost totals, and reports the final journal digest. An unsealed journal is recoverable after a disconnect but is not successful evidence. A sealed journal is immutable and cannot resume.

## Safety and incidents

- Shadow mode remains read-only and uses the established simulated adapter.
- No API credential is required for offline replay.
- Duplicate delivery is idempotent.
- Corruption, sequence gaps, out-of-order records, naive timestamps, binding drift, and post-seal writes fail closed.
- Existing reconciliation and incident streams are preserved inside the chain.
