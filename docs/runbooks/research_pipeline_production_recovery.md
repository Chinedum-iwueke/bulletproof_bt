# Research Pipeline Production Recovery

This runbook is for the hosted Invariance Research pipeline using `bulletproof_bt` as the deterministic engine substrate.

The production rule is simple: never repair a research run by editing derived results. Recover by preserving the failed artifact set, requeueing from the approved experiment contract, and writing a new manifest-backed attempt.

## Required Artifacts Per Experiment

Every hosted research experiment must preserve these files in the SaaS artifact path:

- `experiment_contract.json`
- `run_config.json`
- `execution_manifest.json`
- `verdict.json`
- `verdict_cards.json`
- worker stdout/stderr log artifact

The execution manifest must identify:

- experiment plan id
- experiment item id
- strategy spec id
- hypothesis version id
- runtime limits
- status
- generated timestamp
- output artifact names

If any of these artifacts are missing, the SaaS layer should treat the run as incomplete and keep the job recoverable rather than presenting it as a completed verdict.

## Preflight

Before allowing the SaaS experiment worker to run jobs:

```bash
bt experiment validate examples/experiment_plans/trend_continuation_plan_reference.json
```

For a full contract smoke test:

```bash
pytest tests/test_research_spec_contracts.py tests/test_run_manifest_artifact.py tests/test_artifact_deterministic_serialization.py
```

These checks prove that strategy/research contracts still validate, execution manifests are emitted, and deterministic artifact serialization has not regressed.

## Recovery Procedure

When a hosted experiment job is stuck or failed:

1. Inspect the SaaS job event trail first. Do not edit engine artifacts.
2. Confirm the approved experiment item still points to the same hypothesis/spec/plan lineage.
3. Check whether the worker produced `execution_manifest.json`.
4. If the manifest exists, preserve the failed artifact directory as the failed attempt.
5. Requeue through SaaS Admin Maintenance or the job retry control.
6. Confirm the retry writes a new attempt directory or clearly overwrites only files that belong to the retry job id.
7. Compare the retry manifest to the original contract before trusting the verdict.

If the job repeatedly fails with the same contract, retire or revise the experiment plan rather than force-running the same invalid plan.

## Resume And Determinism Rules

- Backtest semantics must remain event-driven and deterministic.
- No lookahead, interpolation, or hidden execution assumptions may be introduced during recovery.
- Runtime limits should fail closed. A timeout should produce a failure card and manifest status, not a partial success verdict.
- A retry should be linked to the same approved experiment item but must keep a visible attempt history in the SaaS event log.
- Data profile validation failures should stop execution before diagnostics are interpreted.

## Resource Limits

The SaaS layer currently maps `runtime_budget.max_variants` to compute units. Engine-side plans should keep `max_minutes`, `max_variants`, and required datasets explicit so the SaaS scheduler can reject oversized work before it reaches the worker.

Heavy jobs should be marked for explicit human approval once the engine exposes richer runtime estimates.

## Operator Checklist

Use this checklist before inviting additional users:

- `bt experiment validate` passes against reference plans.
- Manifest contract tests pass.
- Deterministic serialization tests pass.
- A failed experiment produces failure cards.
- A retry creates a new visible event trail.
- The SaaS Admin Jobs page can show, retry, and recover experiment jobs.
- The SaaS Admin Health page shows a fresh experiment-worker heartbeat.
