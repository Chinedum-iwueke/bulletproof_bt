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

### Event-Support Planning

Identical `(gate, partition, quantile, threshold_policy)` entries are assessed
once. Reports retain input, unique and duplicate counts; distinct gates,
partitions, thresholds and tail directions remain distinct. Validation precedes
deduplication, so a malformed repeated entry still fails. Input contracts are not
mutated. This makes shared support checks across variants idempotent, without
changing variant budgets or any event minimum.

Each assessment reports `minimum_bars_for_nominal_event_count`, the ceiling of
the event minimum divided by the declared tail mass. For example, 40 events in
a 5% tail require at least 800 nominal bars, before exclusions. This is a planning
diagnostic, not observed support or statistical power. Increasing a frozen split
minimum still requires a reviewed successor; the planner does not alter it.

Local replay of the retained SPX assignment reduced 24 entries to 18 distinct
checks and removed its duplicate-entry exception. It still deferred the original
scientific window and proposed a coverage successor. This is offline validation
of modified source, not deployment, approval or a successful backtest.

**Smoke** has feature warmup, a bounded decision interval of at most 31 days,
and a separate target-horizon tail. Required coverage and compute include all
three; the tail does not increase the decision budget. The execution window
ends at the tail boundary so final decisions can resolve inside admitted data.
This planning reservation is not proof that an implementation correctly excludes
tail rows from signal decisions; runner and evaluator tests must verify that.
The assignment runner now computes its smoke representation splits and leakage
audit only from completed decisions inside the frozen smoke interval. It retains
the caller's complete frame for outcome construction. Regression tests cover
one-, four- and twelve-bar tails. The native engine now supports an explicit
`data.signal_generation_end_ts`: at and after that source timestamp it does not
invoke strategy callbacks, while pending fills and portfolio accounting continue.
The assignment bridge derives this cutoff from the exclusive completed-decision
boundary minus one source minute, not one research bar. Unconfigured historical
runs retain their callback behavior.

Each smoke variant must produce observed callback and tail-bar counts, its last
callback timestamp and its effective cutoff in `decision_logging_summary.json`.
The bridge checks those observations before retaining
`smoke-callback-boundary.json`; absent tail processing, invalid timestamps or a
callback at/after the cutoff fail commissioning. Engine regressions demonstrate
pending fills in the tail, and a public `run_backtest` regression checks override
propagation and accounting through the final bar. These are fixture-based runner
proofs, not a successful real-candidate run or scientific certification. They
also do not prove the correctness of each strategy's target equation or exits.
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

Native requirements planning and candidate-specific coverage production have
been deployed at pinned source `9ad04de`. R10's SPX/BTC candidate reached both
independent requirements reviewers on November 16, 2026. Both rejected its
scientific specification; no engineering task or real-data backtest followed.
This demonstrates the handoff and rejection boundary, not end-to-end certification.
Historical handoffs missing the full reviewed package still fail preflight.

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

### Native planning before paid review

`plan_alpha_requirements.py` checks composed representation history and both
validation modes before independent requirements review. Coverage-only failures
can now produce a bounded `coverage_successor_proposal` using the mandate's
historical envelope and complete governed exposure ledger. The original plan
stays deferred, and neither scientific requirements nor original inputs change.
Separate successor smoke/scientific plans accompany the proposal. The control
plane must validate custody and obtain independent approval before materializing
an engineering task against the successor window. No model is called to choose
dates, and no observed outcomes influence interval selection.

Retained SPX/BTC input replay produces ready successor plans spanning May 14,
2025 through May 18, 2026, including warmup and split gaps. This offline replay
does not certify production review, authoring, execution or a BT-009 outcome.

### Representation semantics repair, not scientific approval

The next local revision adds an explicit stateless `natural_log` operation with
one input and no parameters. It computes the same-bar natural logarithm only for
positive finite values, retaining undefined values as missing. It adds no prior
bar requirement beyond its input's history. Composing `ratio(high, low)` with
`natural_log` expresses a completed-bar log range. `log_return` is deliberately
unchanged: it differences log levels across time and cannot substitute for this
control. Likewise `simple_return` is relative change, not an arithmetic `spread`.
`log1p` separately computes same-bar `ln(1+x)` for finite inputs greater than
minus one, preserving zero-volume observations. It is stateless with no added
history; raw volume and temporal log returns are not substitutes for it.

This operation is tested locally across native materialization and the control
plane/worker contracts, but is not yet released into an approved source-bound
candidate. Existing source-pinned tasks and rejected records remain unchanged.

R10 also exposed a separate support-planning gap. Named event counts are typed,
but the threshold and selection-rate assumptions relating them to partitions
are prose. A 95th-percentile event has nominal rate 0.05: 1,460 training rows imply
73 nominal observations before exclusions, compared with a requirement of 80.
This is not a measured count or a probability guarantee. The local typed
`scientific_specification.quantile_support_plan` now binds each gate, partition
and quantile to `fixed_training_linear_strict_upper_tail` or
`fixed_training_linear_strict_lower_tail`. Native preflight
checks the strict in-sample upper bound `ceil((N - 1) * (1 - q))` for linear
empirical quantiles. For strict lower tails the bound is `ceil((N - 1) * q)`
and nominal rate is `q`, not `1 - q`. Ties or later exclusions can only reduce
that capacity. Both policies retain the original quantile, not a transformed
percentile. Review must verify the declared tail against the event inequality.
Held-out nominal rates remain review warnings, not hard bounds. Coverage
successors cannot bypass an in-sample capacity rejection. Missing designs make
no capacity claim, and rolling/refitted or inclusive threshold policies are not
covered by this contract. Independent review must check applicability and
completeness; implementation and observed counts still require verification.
Do not lower required counts,
relabel missing support as success, or infer qualification from calendar coverage.

R12 commissioning ended as a zero-trial native feasibility deferral: the corrected
6h BTC/SPX rare-tail design required more common history than the approved envelope.
This verifies early rejection, not strategy engineering or backtest certification.
Its frozen representation incorrectly labeled lower-tail support as upper-tail;
the explicit lower-tail contract repair does not alter that immutable run.
A new source-bound proposal must not reuse those incorrect
support declarations or retry the infeasible design unchanged.
