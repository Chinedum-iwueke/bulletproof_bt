# Requirements-Aware Validation

## Contract and Authority

`bt.governance.data_requirements` plans from explicit reviewed requirements,
immutable dataset identities and eligible research-bar coverage. It does not
discover files, call a model, issue approval, place orders or reserve compute.
Its output grants no scientific or execution authority.

An engineering handoff supplies `validation_requirements` with:

- `requirements`: research bar minutes, feature warmup bars, minimum training,
  validation and test bars, target horizon, purge and embargo bars, minimum
  scientific window days, smoke bars, named event-support counts, rationale and
  a reviewed basis digest.
- `coverage`: one `eligible-research-coverage-v1` receipt per exact asset binding.
- `maximum_history_days` and `maximum_bar_variant_work`: approved limits.

The requirement's rationale must distinguish research-specification requirements
from implementation choices. A hidden implementation constant is not permission
to rewrite this contract. Changing a requirement, dataset, window or commit means
a new digest and reviewed successor, not editing an approved task in place.

## Coverage Is Evidence, Not Calendar Arithmetic

`coverage_receipt` compresses sorted, unique, quality-checked completed decision
timestamps into contiguous half-open intervals. It requires the upstream
eligibility evidence digest. Its caller must have checked source-bar completeness,
required fields, point-in-time membership and representation eligibility. Passing
raw timestamps without those checks does not establish valid coverage.

The planner verifies receipt digests and exact dataset bindings, intersects asset
intervals, aligns approval boundaries inward, and selects the most recent adequate
contiguous interval. Missing bars are not filled, and disjoint ranges are not
summed into apparently sufficient history. This initial policy is conservative:
gap-tolerant algorithms require an explicit future contract rather than silently
changing the contiguous requirement.

Receipt digests establish content binding, not authenticity. The admission layer
must supply receipts from its trusted data-quality producer. An LLM must never
invent eligibility receipts, observed event support or permission budgets.

## Two Validation Modes

**Smoke** has feature warmup and a bounded execution interval, at most 31 days.
It establishes runner integration, representation consumption and artifact
retention. Insufficient scientific support can be an expected result. Smoke never
qualifies a strategy or replaces full scientific evaluation.

**Scientific** has separate feature warmup, training, validation and held-out test
partitions. Boundaries preserve target horizon, purge, embargo and target-tail
data. The required window is the larger of declared scientific duration and the
sum of partition minima plus gaps. Extra history goes to training; test and
validation minima are not silently reduced. Warmup is never counted as training.

For the retained TRB example, 31 days supply at most 2,232 twenty-minute bars,
insufficient for 5,000 training observations. A year-long scientific plan with
additional authorized warmup can be feasible. If the authorized data window is
exactly one year, warmup cannot be borrowed from outside it without a successor
approval covering the additional history.

`run_scientific_case` tests unchanged native evaluator logic using full-length
synthetic parquet and the real representation materializer. It produces explicitly
offline evaluator artifacts, not BT-009 receipts. `run_native_case` separately
tests the short commissioning runner. Independent review and full real-data
Bulletproof validation still follow both.

## Preflight and Execution

`scaffold_alpha_strategy.py` now checks both plans before generating a scaffold.
Missing requirements or coverage, insufficient overlap or insufficient budgets
produce a retained, non-retryable preflight report. No coding model is invoked.
The lean worker interpreter needs no pandas/numpy for this check.

The immutable intent includes the requirements package. Scaffolding fixes feature
warmup and split gaps from it. The worker's coverage gate separately observes
runner smoke and scientific evaluator cases; neither can substitute for the other.

For new execution contracts, `verify_execution_plan` recomputes the plan and
requires exact equality with the assignment, source commit and execution window.
Scientific qualification additionally requires that plan in the independently
reviewed artifact bundle. The runner uses the planned partitions instead of its
legacy 60/20/20 split. Historical contracts retain their original semantics.

Declared event support is checked after evaluation and retained in
`requirements-support.json`. Missing counts cannot satisfy support. A shortfall
blocks qualification without rewriting the native finding as a positive result.
Adequate event support alone does not establish an edge.

Work budgeting counts research bars x assets x variants. This is not a RAM or
wall-time estimate; the runtime capacity director must still admit the job.

## Migration and Certification Status

Implemented locally with deterministic planner, runner and worker regressions.
Not deployed or certified for unattended operation. Existing candidate producers
do not yet supply this full reviewed package and eligible-coverage receipt.
Their old handoffs deliberately fail preflight on the new scaffold revision.

Before activation: produce and review genuine requirements/coverage packages for
the two commissioning candidates, bind the resulting plans through qualification
and execution, run both test modes, and obtain independent review plus real-data
BT-009 evidence. Do not start the full queue merely because unit tests pass.

### Producer commissioning, 2026-11-13

`bt.governance.coverage_producer.produce_coverage` now checks selected recovery
bytes and produces bounded OHLCV predictor-availability evidence. Selected-panel
admission can invoke it with an explicit representation and approved window.
It refuses unsupported non-OHLCV availability assumptions and excessive source
row budgets, and reports point-in-time membership as unverified. This is not yet
the final engineering coverage receipt and must not be used to bypass review.

Read-only probes of the genuine TRB and XTZ/RENDER/BTC candidates observed 26122
20-minute and 27066 15-minute usable decisions respectively. Neither covers the
entire frozen requested year. The current canonical panels also end before the
requested endpoint (BTC ends May 18, 2026). Successor windows or new admission,
independent requirement review, membership verification and final plan handoff
remain necessary. No new BT-009 certification is claimed.
