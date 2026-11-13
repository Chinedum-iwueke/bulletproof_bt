# Offline Native Authoring Harness

`bt.validation.authoring_harness` provides reusable fixture-provenance builders and
native-runner assertions. It does not generate profitable strategies, synthesize
review authority, or certify autonomous operation.

## Required Test Structure

1. Construct deterministic OHLCV fixtures from explicit generators. Positive,
   negative, invalid and failed cases need distinct, justified inputs; do not
   choose whichever outcome happened and call that the expected outcome.
2. Call `write_fixture_binding` separately for each fixture asset. It writes the
   parquet and its own catalog, manifest, producer and governance documents. All
   identities describe synthetic fixture bytes, not production lake receipts.
3. Build a separate test contract and a properly bound offline review packet.
   Never monkeypatch `governed_review_verified` or claim that a fixture packet is
   an actual production reviewer decision. The native runner's review checks
   still apply. Test evidence must never be published as a live-data receipt.
4. Preserve scientific semantics: predictor, target, costs, parameters, support,
   minimum history, warmup, timing, representation and production windows. Only
   fixture bindings and their derived intent/evidence digests may change. Nested
   intents must not retain the production dataset identities.
5. Supply enough data, including causal warmup, to meet the actual contract.
   Commissioning execution remains limited to its existing 31-day bound. Choose
   a valid commissioning interval inside the reviewed window; do not rewrite the
   reviewed window to fit a small fixture. If the real gates cannot be exercised
   in that bound, report this limitation rather than reducing them.
6. Call `run_native_case` for a bounded integration smoke outcome. It invokes the real
   `execute_registered` path, rejects runtime replacement of native functions or
   literal constants, and checks result authority, exact outcome, declared metrics,
   complete parameter coverage, record digests and retained artifact digests.
7. Call `run_scientific_case` for positive, negative, invalid and failed synthetic
   evaluator fixtures. It plans adequate scientific history from the frozen
   requirements, materializes representation from separately bound synthetic
   parquet, executes the unchanged evaluator and retains its evidence. This is
   evaluator-only testing, not an engine execution or a production receipt.
8. Separately test mutations of every immutable binding field. Governance or
   content rejections can raise; those exception tests do not count as retained
   scientific invalid/failed outcomes.

`run_native_case` returns an offline case report with
`scientific_certification=false` and `capital_or_order_authority=false`. Its four
successful cases demonstrate infrastructure behavior only. They do not substitute
for independent scientific review, real-data smoke execution or one-year BT-009.

## Detection Boundaries

- The worker observes actual native-runner returns, one completed smoke helper,
  and four completed `run_scientific_case` outcomes before independent review.
- The harness rejects copied production provenance, altered fixture bytes, lowered
  scientific gates, mocked native functions, changed literal constants and replaced
  runner governance functions. Dataset provenance may change only at explicit
  contract paths, not by removing similarly named fields throughout a document.
- Runtime coverage is not an adversarial sandbox or proof that every internal
  calculation is correct. Independent review and broader deterministic tests remain
  necessary. Decorated/dynamic native implementations may require a separately
  reviewed instrumentation extension rather than disabling the check.
- Tests of harness assertions use deliberately constructed result documents to
  exercise rejection rules. Those unit tests are not native-runner evidence.

## Current Validation Status

Harness rejection rules and the worker instrumentation have focused regression
coverage. Successful real-candidate four-outcome runs, independent review, full
validation and BT-009 remain outstanding. Do not mark the autonomous system
certified from these unit tests.

### Known Commissioning Capacity Constraint

The retained TRB G3 implementation requires 5,000 training observations. Its 20m
bars yield at most 2,232 observations in the runner's 31-day commissioning window,
before the train/validation/test split. The runner trims its scientific evaluator
frame to that window; supplying earlier feature warmup does not expand the
evaluator's training sample. A direct replay of the unchanged native `_score`
with 2,232 otherwise eligible observations returned `invalid` with
`minimum_history_not_met` on 2026-11-12.

Consequently, four scientific terminal cases cannot be required through the short
commissioning runner. The requirements-aware repair separates that runner smoke
from full-length synthetic evaluator tests. Do not lower the 5,000-observation
requirement, extend production execution authority, or call an invalid smoke result
positive certification. See `docs/requirements-aware-validation.md` for the new
contracts, fail-closed migration and remaining production validation.
