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

## Runtime model (Bulletproof_bt production mode)
- Canonical input data is **1m**.
- Hypothesis signal timeframe can be `5m` / `15m` / `1h`.
- Signal indicators and entry logic run on **resampled closed signal bars**.
- Exit/risk monitoring (stop/TP) remains on the **base 1m stream**.
- `T_hold` is interpreted in **signal bars**, not 1m bars.
- For L1-H1, `execution_semantics` locks: `stop_model=fixed_atr_multiple`, `stop_update_policy=frozen_at_entry`, `tp_update_policy=frozen_at_entry`, `atr_source_timeframe=signal_timeframe`, `exit_monitoring_timeframe=1m`.

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
