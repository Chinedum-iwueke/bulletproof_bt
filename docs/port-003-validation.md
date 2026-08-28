# PORT-003 construction validation

PORT-003 extends PORT-001 candidate evidence, PORT-002 dependency evidence and
RISK-001 admissibility with a native benchmark ladder and versioned robust
construction producer. Equal-weight and inverse-volatility risk-budget
benchmarks remain visible beside the selected construction.

The projected-gradient robust mean-variance solver uses declared covariance
shrinkage and uncertainty penalties. Feasibility, deterministic replay,
turnover, sensitivity and weight increments are explicit. An optimizer that
does not improve its benchmark, exceeds limits or loses determinism falls back
to a deterministic valid benchmark. Impossible bounds and invalid covariance
fail closed.

The receipt is construction evidence only. It does not allocate capital,
submit orders or promote a portfolio. Hermes owns the immutable solver catalog,
receipt validation and replay, and performs no optimization.
