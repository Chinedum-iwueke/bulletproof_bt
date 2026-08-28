# Quantitative producer ownership validation

Source commit: `597a7697226b82f4c28f5e152c7df1067babba09`.

Bulletproof now owns the authoritative producers for the fifteen computational
milestones reopened by the 2026-08-28 ownership audit. The producers share the
`bulletproof-producer-receipt-v1.0.0` envelope, which binds the producer and version,
full source commit, input, dataset, configuration, artifact and result digests, exact
result payload and a literal no-allocation/no-capital/no-order/no-promotion boundary.

The retained no-capital pilot emitted and verified exactly one receipt for:

- DATA-001 through DATA-003;
- DISC-002 through DISC-005 and DISC-007;
- ML-002 through ML-004;
- RL-001 through RL-002; and
- RISK-001 through RISK-002.

The report is
[`native-producer-report.json`](evidence/quantitative-ownership/native-producer-report.json).
Its canonical report digest is
`5fc1b293fcc0e1cebcdc29fa7fe36b36d0f8ee99a081f650651ce3dae054bb2e`;
the retained file SHA-256 is
`9580ac72704c5bdbe9f63a3de135fba09c67e0c48ab20da61b874748c83a6ea5`.

Verification on Python 3.11:

- focused producer suite: 10 passed;
- full Bulletproof suite: 1,304 passed, 27 explicitly skipped;
- Ruff over the producer package, pilot and focused tests: clean.

The fixture proves producer ownership, deterministic receipt construction, causal and
support gates, failure behavior and cross-domain composition. It is not evidence of
alpha, production data quality, model skill, portfolio admission or trading readiness.
