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

Evidence is retained under `docs/evidence/port-001/` after the implementation source commit is fixed.
