# L1-H11B-REFINED

Claim: H11B pullback geometry has a volatile `1h` continuation edge and a narrower `15m` funding-squeeze edge, but both need explicit R protection and runner trailing.

Data inputs: canonical `research_panel` on 1m execution bars, closed `15m` or `1h` HTF context, and enriched entry-state fields when available. Funding, basis, liquidity, displacement, and CSI are decision-time gates. Missing enriched state means no gated refined entry.

Signal rules: preserve the H11B variable pullback-depth and swing-distance continuation entry, then apply profile-specific side/state gates. Entries require tradeable symbols and closed HTF bars only.

Exit rules: explicit engine stop at entry, break-even after +1R, profile-specific profit lock, profile-specific favorable-extreme runner trail, and trend-failure exit. Exit monitoring is 1m and exits are close-only.

Risk controls: engine canonical R, `r_per_trade=0.005`, no pyramiding, no direct strategy sizing, engine notional/gross exposure/leverage/margin/cost controls remain authoritative.

Grid: four rows, two `1h` and two `15m`, defined in `research/hypotheses/l1_h11b_refined.yaml`.

Logged artifacts: decision traces, entry/exit metadata, explicit stops, geometry parameters, state gates, rich `entry_state_*` fields, path MFE/MAE, counterfactual labels, performance, structural buckets, truth reports, and research extraction inputs.

Evaluation metrics: EV in R, drawdown, top-trade-removed EV, funding-squeeze robustness, symbol concentration, daily/weekly loss behavior, cost drag, and stable/volatile split behavior.

Falsification: reject or refine if the `1h` edge does not survive top-trade removal, the `15m` squeeze pocket is outlier-dependent, stable rejection worsens, state fields are unavailable for gated profiles, or truth validation fails.

Expected failure modes: funding-squeeze overfit, volatile-only regime dependency, cost drag, and runner trail choking right-tail trades.

Compiled feature kernel: none added for this refinement. Execution remains classic engine only; fast-path/full execution kernels require separate parity proof before enablement.
