# Hypothesis Contract

## Purpose
`HypothesisContract` is the pre-registered research protocol used to execute hypothesis variants deterministically, enforce execution-aware tier requirements, and emit standardized machine-readable logs.

## YAML structure
Each contract YAML defines:
- metadata (`hypothesis_id`, `title`, `research_layer`, `version`, etc.)
- `required_indicators`
- `parameter_grid` (explicit immutable value lists)
- optional `gates`, `entry`, and `exit` sections
- optional `execution_semantics` section for explicit two-clock and stop/TP update policy locks
- `evaluation.required_tiers` (defaults to `Tier2` + `Tier3`)
- `logging` schema hints
- optional `runtime_controls` such as `max_variants`

## Runtime model (bulletproof_bt production mode)
- Canonical input data is **1m**.
- Hypothesis signal timeframe can be `5m` / `15m` / `1h`.
- Signal indicators and entry logic run on **resampled closed signal bars**.
- Exit/risk monitoring (stop/TP) remains on the **base 1m stream**.
- `T_hold` is interpreted in **signal bars**, not 1m bars.
- For L1-H1, `execution_semantics` locks: `stop_model=fixed_atr_multiple`, `stop_update_policy=frozen_at_entry`, `tp_update_policy=frozen_at_entry`, `atr_source_timeframe=signal_timeframe`, `exit_monitoring_timeframe=1m`.
- For L1-H3, `execution_semantics` locks: `signal_timeframe=15m`, `base_data_frequency_expected=1m`, `gate_model=har_rv_percentile`, `stop_model=fixed_close_sqrt_rvhat_multiple`, `coefficient_refit_cadence=daily_on_completed_signal_day`, and `fit_method=deterministic_ols`.
- For L1-H2, `execution_semantics` additionally locks: `signal_timeframe=5m`, `vwap_mode=session`, `profit_exit_model=vwap_touch`, `hold_time_unit=signal_bars`, and `no_pyramiding=true`.
- For L1-H3B, `execution_semantics` additionally locks: `signal_timeframe=5m`, `base_data_frequency_expected=1m`, `strategy_family=mean_reversion`, `baseline_reference=L1-H2`, `gate_model=har_rv_percentile_low`, `stop_model=fixed_close_sqrt_rvhat_multiple`, `vwap_mode=session`, `profit_exit_model=vwap_touch`, `coefficient_refit_cadence=daily_on_completed_signal_day`, and `fit_method=deterministic_ols`.
- For L1-H3C, `execution_semantics` additionally locks: `strategy_family=regime_switch`, `baseline_references=[L1-H1,L1-H2]`, `gate_model=har_rv_percentile_switch`, `branch_allocation_clock=15m_completed_bars`, `branch_high_vol.signal_timeframe=15m`, `branch_low_vol.signal_timeframe=5m`, `stop_model=fixed_close_sqrt_rvhat_multiple`, `stop_update_policy=frozen_at_entry`, `coefficient_refit_cadence=daily_on_completed_signal_day`, and `fit_method=deterministic_ols`.
- For L1-H4A, `execution_semantics` additionally locks: `signal_timeframe=5m`, `base_data_frequency_expected=1m`, `strategy_family=mean_reversion`, `baseline_reference=L1-H2`, `liquidity_proxy=bar_range_half_over_close`, `gate_model=spread_proxy_quantile_gate`, `vwap_mode=session`, `stop_model=fixed_atr_multiple`, `stop_update_policy=frozen_at_entry`, `profit_exit_model=vwap_touch`, `hold_time_unit=signal_bars`, and `no_pyramiding=true`.
- For L1-H4B, `execution_semantics` additionally locks: `signal_timeframe=5m`, `base_data_frequency_expected=1m`, `strategy_family=mean_reversion`, `baseline_reference=L1-H4A`, `liquidity_proxy=bar_range_half_over_close`, `gate_model=spread_proxy_quantile_gate`, `size_adjustment_model=capped_inverse_spread_proxy_ratio`, `spread_proxy_reference_model=rolling_median`, `vwap_mode=session`, `stop_model=fixed_atr_multiple`, `stop_update_policy=frozen_at_entry`, `profit_exit_model=vwap_touch`, `hold_time_unit=signal_bars`, and `no_pyramiding=true`.
- For L1-H5A, `execution_semantics` additionally locks: `signal_timeframe=15m`, `base_data_frequency_expected=1m`, `strategy_family=trend_continuation`, `baseline_reference=L1-H1`, `overlay_type=volatility_managed_exposure`, `vol_estimator=sqrt_mean_squared_returns`, `sigma_reference_model=rolling_median`, `sizing_model=qty_R_times_clipped_inverse_vol`, `stop_model=fixed_atr_multiple`, `stop_update_policy=frozen_at_entry`, `hold_time_unit=signal_bars`, and `no_pyramiding=true`.
- For L1-H5B, `execution_semantics` additionally locks: `signal_timeframe=15m`, `base_data_frequency_expected=1m`, `strategy_family=trend_continuation`, `baseline_reference=L1-H3A`, `gate_model=har_rv_percentile`, `overlay_type=volatility_managed_exposure`, `vol_estimator=sqrt_mean_squared_returns`, `sigma_reference_model=rolling_median`, `sizing_model=qty_R_times_clipped_inverse_vol`, `stop_model=fixed_close_sqrt_rvhat_multiple`, `stop_update_policy=frozen_at_entry`, `coefficient_refit_cadence=daily_on_completed_signal_day`, `fit_method=deterministic_ols`, `hold_time_unit=signal_bars`, and `no_pyramiding=true`.

## Deterministic materialization
Grid expansion uses a stable cartesian product over sorted parameter keys. Each variant receives:
- `grid_id` (`g00000`, `g00001`, ...)
- `config_hash` (canonical SHA-256 over sorted JSON params)

No variants are dropped silently; invalid grids fail loudly.

## Tier enforcement
`HypothesisContract` defaults required tiers to `Tier2` and `Tier3` when unspecified.
`run_hypothesis_contract` checks available tiers before execution and raises `MissingRequiredTierError` if any required tier is missing.

## Standard logging schema
Each run record includes canonical fields such as:
`run_id`, `hypothesis_id`, `grid_id`, `config_hash`, symbol/timeframe window, tier, params/indicator/gate snapshots, core metrics, status, and failure reason.

## CLI
Use the production runner CLI with explicit runtime paths:

```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1_tier2 \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --phase tier2
```

```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1_tier3 \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --phase tier3
```

```bash
python -m bt.experiments.hypothesis_runner \
  --config configs/engine.yaml \
  --local-config configs/local/engine.lab.yaml \
  --data /home/omenka/research_data/bt/curated/stable_data_1m_canonical \
  --out outputs/l1_h1_validate \
  --hypothesis research/hypotheses/l1_h1_vol_floor_trend.yaml \
  --phase validate
```
