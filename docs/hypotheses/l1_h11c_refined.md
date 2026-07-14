# L1-H11C-REFINED

Claim: H11C's protection discipline is useful mainly in volatile `1h` continuation states; broad `15m` remains weak except for narrow diagnostic pockets.

Data inputs: canonical `research_panel` on 1m execution bars, closed `15m` or `1h` HTF context, session VWAP, and enriched entry-state fields when available. Funding, basis, liquidity, displacement, and CSI are decision-time gates. Missing enriched state means no gated refined entry.

Signal rules: preserve the H11C structure-plus-padding continuation entry, then apply profile-specific side/state gates. Entries require tradeable symbols and closed HTF bars only.

Exit rules: structure-plus-padding engine stop at entry, break-even after +1R, profile-specific profit lock, profile-specific favorable-extreme runner trail, optional VWAP giveback where configured, and trend-failure exit. Exit monitoring is 1m and exits are close-only.

Risk controls: engine canonical R, `r_per_trade=0.005`, no pyramiding, no direct strategy sizing, engine notional/gross exposure/leverage/margin/cost controls remain authoritative.

Grid: four rows, two `1h` and two `15m`, defined in `research/hypotheses/l1_h11c_refined.yaml`.

Logged artifacts: decision traces, entry/exit metadata, explicit stops, protection parameters, state gates, rich `entry_state_*` fields, path MFE/MAE, counterfactual labels, performance, structural buckets, truth reports, and research extraction inputs.

Evaluation metrics: EV in R, drawdown, top-trade-removed EV, exit capture, symbol concentration, daily/weekly loss behavior, cost drag, and stable/volatile split behavior.

Falsification: reject or refine if protection does not improve exit capture, the `15m` rescue pockets are outlier-dependent, stable rejection worsens, state fields are unavailable for gated profiles, or truth validation fails.

Expected failure modes: over-protection, VWAP giveback choking winners, narrow 15m state overfit, and volatile-only regime dependency.

Compiled feature kernel: none added for this refinement. Execution remains classic engine only; fast-path/full execution kernels require separate parity proof before enablement.
