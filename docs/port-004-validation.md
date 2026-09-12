# PORT-004 turnover, cost, capacity and liquidity validation

PORT-004 composes the admitted PORT-003 construction with EXEC-005 execution
calibration. Bulletproof remains the sole quantitative producer. Hermes may register
the schema and exact native receipt but does not calculate turnover, costs, liquidity
or capacity.

The producer uses only liquidity snapshots available by the declared knowledge
cutoff. It reports gross-L1 and one-way turnover, deterministic base and stressed cost
curves, and participation, visible-depth, cost and liquidation capacity constraints by
candidate. Venue, listing, order type, size bucket and regime remain explicit.

Missing, stale, future-only or mismatched evidence cannot inherit capacity. The result
is `abstain_zero_executable_capacity`; stressed costs cannot improve on base costs and
stressed capacity cannot exceed base capacity. The receipt grants no allocation,
capital, order or promotion authority. ML-005 may later provide admitted conditional
models, but PORT-004 does not fabricate one.
