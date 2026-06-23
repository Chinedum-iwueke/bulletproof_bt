# Research Memory Report

## Executive Summary

- Trades in memory: 66838
- Under-instrumented or invalid trades recorded but excluded from recommendations: 0
- State buckets: 1480
- Alpha candidates/verdict records: 1
- Proposed recommendations requiring human approval: 2086

## What States Currently Show Edge

- state_key=funding_x_basis, bucket=entry_state_funding_pctile_b3__entry_state_basis_pctile_b0, setup_class=None, n_trades=478, ev_r_net=2.240833277444711, avg_cost_drag_r=0.0327982990968807
- state_key=funding_x_basis, bucket=entry_state_funding_pctile_b3__entry_state_basis_pctile_b0, setup_class=None, n_trades=478, ev_r_net=2.2408332774, avg_cost_drag_r=0.0327982991
- state_key=liquidity_x_funding, bucket=entry_state_spread_proxy_pctile_b1__entry_state_funding_pctile_b3, setup_class=None, n_trades=427, ev_r_net=2.035106602539417, avg_cost_drag_r=0.0338870480689742
- state_key=liquidity_x_funding, bucket=entry_state_spread_proxy_pctile_b1__entry_state_funding_pctile_b3, setup_class=None, n_trades=427, ev_r_net=2.0351066025, avg_cost_drag_r=0.0338870481
- state_key=displacement_x_liquidity, bucket=extreme_impulse__entry_state_spread_proxy_pctile_b1, setup_class=None, n_trades=430, ev_r_net=2.0175887537, avg_cost_drag_r=0.0266649188
- state_key=displacement_x_liquidity, bucket=extreme_impulse__entry_state_spread_proxy_pctile_b1, setup_class=None, n_trades=430, ev_r_net=2.01758875365093, avg_cost_drag_r=0.0266649187676116
- state_key=setup_x_csi, bucket=quality_filtered_continuation__csi_high, setup_class=None, n_trades=500, ev_r_net=1.8240609461, avg_cost_drag_r=0.0307443688
- state_key=setup_x_csi, bucket=quality_filtered_continuation__csi_high, setup_class=None, n_trades=500, ev_r_net=1.8240609460785104, avg_cost_drag_r=0.03074436883356
- state_key=csi_x_oi, bucket=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2, setup_class=None, n_trades=33, ev_r_net=1.8238241837054547, avg_cost_drag_r=0.1331675139191212
- state_key=csi_x_oi, bucket=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2, setup_class=None, n_trades=33, ev_r_net=1.8238241837, avg_cost_drag_r=0.1331675139

## What States Should Be Avoided

- state_key=csi_x_oi, bucket=entry_state_csi_pctile_b3__entry_state_oi_accel_pctile_b0, setup_class=None, n_trades=23, ev_r_net=-1.378453396641348, avg_cost_drag_r=0.0319474697065652
- state_key=csi_x_oi, bucket=entry_state_csi_pctile_b3__entry_state_oi_accel_pctile_b0, setup_class=None, n_trades=23, ev_r_net=-1.3784533966, avg_cost_drag_r=0.0319474697
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b2__entry_state_oi_accel_pctile_b2, setup_class=None, n_trades=42, ev_r_net=-1.3602781849, avg_cost_drag_r=0.1462536854
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b2__entry_state_oi_accel_pctile_b2, setup_class=None, n_trades=42, ev_r_net=-1.360278184879762, avg_cost_drag_r=0.1462536853742381
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b3__entry_state_oi_accel_pctile_b0, setup_class=None, n_trades=30, ev_r_net=-1.2869499957200332, avg_cost_drag_r=0.0233441371651333
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b3__entry_state_oi_accel_pctile_b0, setup_class=None, n_trades=30, ev_r_net=-1.2869499957, avg_cost_drag_r=0.0233441372
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b3__entry_state_oi_accel_pctile_b0, setup_class=None, n_trades=81, ev_r_net=-1.1953138948607716, avg_cost_drag_r=nan
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b3__entry_state_oi_accel_pctile_b3, setup_class=None, n_trades=12, ev_r_net=-1.18198808883325, avg_cost_drag_r=0.1397281652455833
- state_key=funding_x_oi, bucket=entry_state_funding_pctile_b3__entry_state_oi_accel_pctile_b3, setup_class=None, n_trades=12, ev_r_net=-1.1819880888, avg_cost_drag_r=0.1397281652
- state_key=funding_x_basis, bucket=entry_state_funding_pctile_b3__entry_state_basis_pctile_b0, setup_class=None, n_trades=452, ev_r_net=-1.1105304907, avg_cost_drag_r=0.1203238732

## Best Setup Classes

_No evidence yet._

## Weak / Rejected Setup Classes

- setup_class=quality_filtered_continuation, n_trades=66838, ev_r_net=-0.12380982679622317, avg_mfe_r=2.450692497883417, tail_5r_count=2743

## Cost Fragility Map

- setup_class=quality_filtered_continuation, n_trades=66838, ev_r_net=-0.12380982679622317, avg_cost_drag_r=0.07459196583271975

## Exit Failure Map

- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_oi=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.997244426548479, confidence=0.7666666666666667
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_oi=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.9972444265, confidence=0.7666666666666667
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for funding_x_oi=entry_state_funding_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.2537564768, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for funding_x_oi=entry_state_funding_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.253756476778947, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_oi=extreme_impulse__entry_state_oi_accel_pctile_b3., evidence_score=3.1746319317, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_oi=extreme_impulse__entry_state_oi_accel_pctile_b3., evidence_score=3.1746319316727623, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_liquidity=mild_impulse__entry_state_spread_proxy_pctile_b0., evidence_score=3.112705515, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_liquidity=mild_impulse__entry_state_spread_proxy_pctile_b0., evidence_score=3.1127055149528067, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b3., evidence_score=3.03210856032, confidence=1.0
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b3., evidence_score=3.0321085603, confidence=1.0

## Alpha Candidates Worth Tier3 Review

_No evidence yet._

## Recommended Gates

- setup_class=None, recommendation=Add gate favoring setup_x_csi=quality_filtered_continuation__csi_high., evidence_score=1.0, confidence=1.0
- setup_class=None, recommendation=Add gate favoring displacement_x_liquidity=extreme_impulse__entry_state_spread_proxy_pctile_b1., evidence_score=1.0, confidence=1.0
- setup_class=None, recommendation=Add gate favoring setup_x_csi=quality_filtered_continuation__csi_high., evidence_score=1.0, confidence=1.0
- setup_class=None, recommendation=Add gate favoring displacement_x_liquidity=extreme_impulse__entry_state_spread_proxy_pctile_b1., evidence_score=1.0, confidence=1.0
- setup_class=None, recommendation=Add gate favoring setup_x_csi=quality_filtered_continuation__csi_high., evidence_score=1.0, confidence=1.0
- setup_class=None, recommendation=Add gate favoring displacement_x_liquidity=extreme_impulse__entry_state_spread_proxy_pctile_b1., evidence_score=0.8652314074763112, confidence=1.0
- setup_class=None, recommendation=Add gate favoring csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b2., evidence_score=0.635283346, confidence=1.0
- setup_class=None, recommendation=Add gate favoring csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b2., evidence_score=0.6352833459847715, confidence=1.0
- setup_class=None, recommendation=Add gate favoring vol_x_liquidity=entry_state_vol_pctile_b3__entry_state_spread_proxy_pctile_b1., evidence_score=0.5978506961, confidence=1.0
- setup_class=None, recommendation=Add gate favoring vol_x_liquidity=entry_state_vol_pctile_b3__entry_state_spread_proxy_pctile_b1., evidence_score=0.5978506960621714, confidence=1.0

## Recommended Sizing Adjustments

_No evidence yet._

## Recommended Exit Refinements

- setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_oi=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.997244426548479, confidence=0.7666666666666667
- setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_oi=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.9972444265, confidence=0.7666666666666667
- setup_class=None, recommendation=Test trailing or chandelier exits for funding_x_oi=entry_state_funding_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.2537564768, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for funding_x_oi=entry_state_funding_pctile_b0__entry_state_oi_accel_pctile_b2., evidence_score=3.253756476778947, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_oi=extreme_impulse__entry_state_oi_accel_pctile_b3., evidence_score=3.1746319317, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_oi=extreme_impulse__entry_state_oi_accel_pctile_b3., evidence_score=3.1746319316727623, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_liquidity=mild_impulse__entry_state_spread_proxy_pctile_b0., evidence_score=3.112705515, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_liquidity=mild_impulse__entry_state_spread_proxy_pctile_b0., evidence_score=3.1127055149528067, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b3., evidence_score=3.03210856032, confidence=1.0
- setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b3., evidence_score=3.0321085603, confidence=1.0

## Research Gaps

- Treat thin samples as research prompts, not approval signals.
- Re-run memory after new Tier2/Tier3 batches or daily scheduled research windows.
- Inspect invalid metrics runs before using their evidence.

## Next Tests To Queue Manually

- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_oi=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_oi=entry_state_csi_pctile_b0__entry_state_oi_accel_pctile_b2.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for funding_x_oi=entry_state_funding_pctile_b0__entry_state_oi_accel_pctile_b2.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for funding_x_oi=entry_state_funding_pctile_b0__entry_state_oi_accel_pctile_b2.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_oi=extreme_impulse__entry_state_oi_accel_pctile_b3.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_oi=extreme_impulse__entry_state_oi_accel_pctile_b3.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_liquidity=mild_impulse__entry_state_spread_proxy_pctile_b0.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for displacement_x_liquidity=mild_impulse__entry_state_spread_proxy_pctile_b0.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b3.
- recommendation_type=REFINE_EXIT, setup_class=None, recommendation=Test trailing or chandelier exits for csi_x_liquidity=entry_state_csi_pctile_b0__entry_state_spread_proxy_pctile_b3.
