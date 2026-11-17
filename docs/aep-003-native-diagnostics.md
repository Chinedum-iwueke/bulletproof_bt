# AEP-003 native diagnostics

Status: diagnostic substrate implemented and tested; no production source
transition or scientific certification. Method-episode orchestration is AEP-004.

Existing native tools to reuse:
- `requirements_feasibility.plan_requirements`: composed feature history,
  smoke/scientific partitions, covered successor windows and support checks.
- `event_support_planning.assess_event_support`: strict training-quantile bounds,
  held-out nominal capacity and explicit non-claims about observed support/power.
- `data_requirements.preflight_evidence`: exact source, coverage and bounded
  execution planning from requirements. These are not substitute power analyses.

New `design_adequacy.assess_design_adequacy` validates a proposed contract containing
estimand, estimator identity, meaningful effect value/units/justification/references,
dependence assumptions, multiplicity, eligibility, undefined-replicate policy and
a seeded calibration budget. It returns subject/source/contract-bound assessment
digests, never scientific acceptance or execution authority.

Evidence references are syntax-checked, not authenticated; estimator and DGP IDs
are proposals, not a registered implementation. Their custody and implementation
bindings remain explicit unverified obligations. Replica-product bounds are not
actual CPU/RAM estimates and do not authorize launching simulations. This contract
cannot infer justified effect thresholds or statistically adequate replicate counts.

Validation covers design adequacy, event support, requirements feasibility,
diagnostic dispatch, bootstrap behavior and CLI source custody.
No market data, external model, order or campaign
was invoked. Synthetic effect values in tests are not proposed trading criteria.

## Registry and execution boundary

`diagnostic_tools.run_diagnostic` exposes four fixed tool IDs:
- `requirements_feasibility_v1`: native history, split, timing and coverage planning.
- `event_support_v1`: exact training-quantile limits and labelled nominal support.
- `design_adequacy_v1`: proposal completeness, not scientific acceptance.
- `residual_calibration_v1`: seeded calibration of the registered frozen residual
  contrast core, not a fitted strategy or the candidate's full joint procedure.

Requests bind source commit, candidate subject, tool/version and payload. Evidence
documents must match supplied hashes and all justification references must exist.
This establishes content integrity, not truth or authority of a submitted source.
Custody comes from the existing authenticated admission task and independent review.
Unknown/disabled tools and unsupported estimator/DGP/policy combinations return
`await_engineering` with explicit approval required; they never fall back to an
approximation or execute model-provided code. Runtime disable lists are caller-owned.

All JSON is bounded to 256 KiB. Calibration is bounded by an explicit work count
including rows, block construction and bootstrap draws, with a hard ten-million
unit ceiling. Production subprocess deadlines remain the worker's responsibility.
Implementation hashes, seed/configuration, input and evidence catalog digests are
retained. Receipts never grant scientific approval or execution authority.

The standalone `scripts/run_native_diagnostic.py` reads a request/catalog envelope
from stdin and emits JSON. It checks actual Git HEAD and refuses dirty native code.
No request can select a shell command, module import, arbitrary data path or network
operation. Existing native feasibility tasks can opt in via `diagnostic_request`
and `diagnostic_evidence_catalog`; their candidate digest must match. Unavailable
tools defer the native report rather than letting it claim readiness.

Hermes `native_diagnostics.verify_diagnostic` validates the resulting custody chain
through `alpha_requirements_feasibility.verified_report`. Missing, unsolicited,
altered, wrong-subject/source receipts and nested authority claims fail closed.
Existing inputs without a diagnostic request retain their original behavior.

## Calibration scope

Supported estimator: `frozen_residual_contrast_v1`. Supported scenarios are
`iid_null_v1`, `ar_null_v1` (rho .6) and `iid_shift_v1`. Events are IID Bernoulli .1;
the Gaussian residual recursion starts at stationary unit variance. The shift adds
the specified residual-SD effect to primary events. Both models use aligned rows.
Results report primary marginal rejection/coverage, valid and invalid experiments,
unconditional and conditional denominators, and Wilson intervals for Monte Carlo
uncertainty. Undefined bootstrap groups invalidate the experiment, never silently
drop draws. Linear percentile interpolation and centered p-values are explicit.

Only `single_primary_one_sided_0.05`, `all_synthetic_rows`, and
`invalidate_experiment` policies are implemented. A request for joint Holm,
different eligibility or a full fitted OLS estimator must obtain an engineering
implementation and review; this core cannot stand in for it. There is no universal
meaningful-effect threshold or sample-size approval hidden in this tool.

Acceptance remains the end-to-end program's evidence assessment. No real candidate
has been scientifically approved, modified or executed by this increment.
