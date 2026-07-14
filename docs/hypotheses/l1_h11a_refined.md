# L1-H11A-REFINED

Claim: H11A continuation becomes deployment-research-worthy only in volatile, state-filtered pockets where early R protection and runner trailing prevent large MFE giveback.

Data inputs: canonical `research_panel` on 1m execution bars, closed `15m` or `1h` HTF context, and enriched entry-state fields when available. Funding, basis, liquidity, displacement, and CSI are decision-time signal gates for refined profiles. Missing enriched state means no gated refined entry, not a fallback entry.

Signal rules: preserve the H11A EMA20/EMA50 pullback-reclaim continuation entry, then apply the profile side/state gates. Entries require tradeable symbols and closed HTF bars only.

Exit rules: explicit engine stop at entry, break-even after +1R, profit lock after profile-specific MFE, runner trail after profile-specific MFE, and trend-failure exit. Exit monitoring is 1m and exits are close-only.

Risk controls: engine canonical R, `r_per_trade=0.005`, no pyramiding, no direct strategy sizing, engine notional/gross exposure/leverage/margin/cost controls remain authoritative.

Grid: four rows, two `1h` and two `15m`, defined in `research/hypotheses/l1_h11a_refined.yaml`.

Logged artifacts: decision traces, entry/exit metadata, explicit stops, state gates, rich `entry_state_*` fields, path MFE/MAE, counterfactual labels, performance, structural buckets, truth reports, and research extraction inputs.

Evaluation metrics: EV in R, drawdown, top-trade-removed EV, state-pocket stability, symbol concentration, daily/weekly loss behavior, cost drag, and stable/volatile split behavior.

Falsification: reject or refine if volatile edge disappears after top-trade removal, stable rejection is not cleanly understood, drawdown violates prop-style limits, rich state fields are unavailable for gated profiles, or truth validation fails.

Expected failure modes: outlier dependence, volatile-only regime dependency, over-tight runner trail, and state-gate overfit.

Compiled feature kernel: none added for this refinement. Execution remains classic engine only; fast-path/full execution kernels require separate parity proof before enablement.
