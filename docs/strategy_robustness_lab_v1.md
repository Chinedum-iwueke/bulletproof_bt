# Strategy Robustness Lab V1 (bulletproof_bt Service Layer)

## What was implemented

The V1 SaaS backend is now centered on `bt.saas` and provides:

- Trade-log CSV ingestion with normalization, inference, and actionable validation errors.
- Existing run-artifact ingestion (`trades.csv`, `equity.csv`, `performance.json`).
- UI-ready payload generation for all V1 dashboard sections.
- Deterministic Monte Carlo crash-test diagnostics with reproducible seeds.
- Dedicated risk-of-ruin/survivability payloads.
- Composite robustness score (0–100) with transparent sub-scores and explicit weights.
- Consolidated validation report payload suitable for HTML/PDF rendering later.

## Upload schema (trade log)

### Required minimum

- `entry_ts` (or alias: `entry_time`, `timestamp`)
- `symbol`
- `side` (or alias: `direction`; supports BUY/LONG/SELL/SHORT)
- and either:
  - `quantity`/`size` + `entry_price` + `exit_price`, or
  - `pnl` / `pnl_net`

### Optional but supported

- `exit_ts`
- `fees` / `commission`
- `risk_amount`
- `mae_price`
- `mfe_price`
- `r_multiple_net`

### Inference rules

- If `pnl_net` is missing but `entry_price`, `exit_price`, `quantity`, and `side` exist, `pnl_net` is inferred.
- If `r_multiple_net` is missing but `risk_amount` exists, `r_multiple_net = pnl_net / risk_amount`.

## Diagnostics methodology

### Monte Carlo Crash Test

- Method: bootstrap sampling with replacement from realized trade PnL (`pnl_net`).
- Deterministic: uses explicit `seed`.
- Outputs:
  - fan-chart paths (equity trajectories)
  - drawdown distribution
  - worst/median drawdown
  - probability of hitting drawdown thresholds (default 30%, 50%)
  - ruin probability (equity breach of 50% starting equity threshold)

### Execution Sensitivity

- Builds stressed curves by scaling:
  - fees
  - slippage + spread
  - combined execution costs
- Reports break-even cost multiplier and an execution resilience sub-score.

### Regime Analysis (V1 minimum)

- Volatility regime segmentation (rolling PnL volatility tertiles).
- Trend/range proxy segmentation (rolling PnL mean sign).
- Session segmentation (Asia/Europe/US buckets by entry hour).

### Risk of Ruin

- Reuses Monte Carlo outputs to expose:
  - ruin probability
  - probabilities of 30% and 50% drawdown
  - expected worst drawdown
  - capital threshold and optional risk-per-trade scenario fields

## Scoring methodology (V1)

`RobustnessScore =`

- `0.25 * StatisticalQuality`
- `0.25 * MonteCarloStability`
- `0.20 * DrawdownResilience`
- `0.15 * ExecutionResilience`
- `0.15 * ParameterStability`

### Sub-scores exposed

- Statistical Quality
- Monte Carlo Stability
- Drawdown Resilience
- Execution Resilience
- Parameter Stability
- Regime Consistency (supporting, currently non-weighted in V1)

All scores are normalized to 0–100 and serialized with the weights/methodology in payload.

## Public service usage

```python
from bt.saas import StrategyRobustnessLabService

service = StrategyRobustnessLabService()
run = service.ingest_trade_log("uploads/my_trades.csv", strategy_name="my_strategy")
payload = service.build_dashboard_payload(run, seed=42, simulations=1000)
```

Or ingest existing bulletproof_bt run artifacts:

```python
run = service.ingest_run_artifacts("outputs/runs/run_20260101_120000")
payload = service.build_dashboard_payload(run)
```

## Intentional V1.1 deferrals

- Full bar-level regime tagging joined from market data for every uploaded trade.
- Rich parameter-neighborhood diagnostics beyond grid-summary heatmap interpretation.
- Branded HTML/PDF renderers (payload contract is ready; renderer not included).
