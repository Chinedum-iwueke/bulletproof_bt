# Backtest Truth Certification

Bulletproof_bt treats strategy generation, execution, and result publication as
separate trust boundaries. A well-written prompt is useful, but it is not a
security or accounting boundary.

## Trust Chain

1. `truth_contract` fixes causal, execution, risk, and accounting authority.
2. `validate_hypothesis_admission.py` rejects an unsafe hypothesis/strategy
   package before workers launch.
3. The classic engine remains authoritative for clocks, fills, costs, stops,
   sizing, margin, and accounting.
4. `validate_experiment_truth.py` reconciles every completed run before
   analysis and again after canonical extraction.
5. Cleanup cannot substitute for certification; only explicitly recorded
   post-extraction log deletion is accepted.
6. Verdicts, terminal cards, state discovery, and research memory run only
   after both stable and volatile truth gates pass.
7. SaaS consumers call `require_truth_certification()` before exposing an
   engine-generated experiment as certified.

## Failure Policy

The system fails closed. Missing evidence, future source timestamps, stale or
divergent identities, invalid R, accounting drift, cap breaches, negative free
margin, incomplete grids, or uncertified fast paths invalidate the experiment.
No partial metric should be marketed as a certified backtest.

Certification proves internal consistency under the declared data and
execution assumptions. It does not prove that historical data is complete,
that future returns will match the backtest, or that an exchange will execute
identically during an unprecedented market event. Those limitations must remain
visible in user-facing reports.
