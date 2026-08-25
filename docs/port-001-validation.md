# PORT-001 Portfolio Candidate and Deterministic Risk Service

PORT-001 adds a non-allocating Book VII service beside Bulletproof's existing portfolio execution engine. It consumes canonical, digest-bound research and shadow evidence from an explicit registry snapshot and produces a replayable portfolio-candidate dossier.

## Contract

- Candidate inputs bind research evidence, prospective shadow evidence, forecast horizon, uncertainty, turnover, costs, stressed capacity, common dependencies, exact-overlap net returns, and named scenarios.
- Unknown overlap, corrupt digests, unregistered evidence, zero variance, incompatible horizons, or incomplete scenarios fail closed.
- Dependency evidence includes exact-overlap covariance and correlation plus shared instrument, venue, data, model, and infrastructure dependencies. The constraint uses the conservative maximum of statistical and shared dependency.
- The transparent benchmark is capacity-capped inverse volatility with uncertainty included in the risk scale. Candidate, family, concentration, dependency, and stress constraints are hard.
- Every accepted or rejected result is content addressed. A dossier is always marked `allocated: false`; portfolio allocation, capital, orders, and self-promotion are prohibited.

This service does not replace `PortfolioRiskCoordinator`, submit orders, mutate positions, or grant portfolio admission. It prepares evidence for independent portfolio and risk review.

## Validation

The focused suite covers deterministic reproduction, public-schema validation, exact-overlap failure, unregistered/corrupt evidence, candidate and family infeasibility, concentration, shared/statistical dependency, cost/regime/capacity stress, and authority tampering.

The retained pilot runs one feasible three-candidate fixture and three rejected paths: missing overlap, infeasible stressed capacity, and a binding loss-stress constraint. All inputs are deterministic fixtures; no production resources or capital-bearing systems are touched.

## Retained result

The implementation source commit is `4eaf836d2b0f9a1de4730b2141847cc7d073e40a`. The complete Bulletproof suite passed with 1,294 tests, 27 declared skips, and two pre-existing pandas warnings. Ruff, MyPy, 26 focused/existing portfolio tests, and the eight-test PORT-001 contract suite passed.

The retained pilot report has digest `98964cfa2fd0afd65208330bf50f6b25c3eff99d9e0faa9723efca0a6843e555`. Its accepted candidate-set digest is `43a0bd21e8c1286b13e1c4de9010c19580e543cd9b2402cdfc0bb31b2e43b325` and its dossier digest is `3ab51287d89cfdc02a04b4ba1014a85a6716b1a55a086da43d74d4867a3378bd`. A second run reproduced all five JSON artifacts byte for byte.

Evidence is retained under `docs/evidence/port-001/`. The fixture proposes no real allocation and touches no production resource.
