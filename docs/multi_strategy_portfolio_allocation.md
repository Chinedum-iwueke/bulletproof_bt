# Multi-Strategy Portfolio Allocation

Multi-strategy portfolio allocation runs several strategy/hypothesis sleeves as one combined portfolio. Each sleeve has its own `strategy_id`, `hypothesis_id`, config, optional universe/data path, and capital bucket.

This differs from single-strategy testing: a single strategy asks, "does this edge work by itself?" A portfolio asks, "what does this group of edges do together, and which sleeve contributes or consumes risk?"

## Config Shape

```yaml
portfolio_id: edge_portfolio_v1
starting_equity: 100000
base_currency: USDT
data_path: data/curated/sample.csv
allocation_policy:
  type: fixed_weight
  rebalance_frequency: daily
  max_strategy_weight: 0.40
  max_symbol_exposure: 0.30
  max_total_gross_exposure: 1.50
  max_total_net_exposure: 1.00
  conflict_policy: block_conflict
strategies:
  - strategy_id: trend_breakout_btc_eth
    hypothesis_id: H001
    enabled: true
    weight: 0.35
    config_path: ../engine.yaml
    overrides:
      strategy:
        name: coinflip
        p_trade: 0.05
```

## Allocation Policies

- `equal_weight`: every enabled sleeve gets the same weight.
- `fixed_weight`: weights come from each strategy entry and are normalized.
- `risk_parity_simple`: inverse-volatility weighting using `expected_volatility`.
- `vol_target`: simple target-vol scaling using `allocation_policy.target_volatility`.
- `manual_capital_buckets`: capital comes from each strategy entry and is normalized to portfolio equity.

Each sleeve is run with isolated initial capital, so one strategy cannot silently consume another strategy's bucket.

## Conflict And Risk Logic

The shared `PortfolioRiskCoordinator` supports:

- max strategy exposure;
- max symbol exposure;
- max portfolio gross/net exposure;
- per-strategy and portfolio position caps;
- per-strategy loss disable;
- portfolio drawdown kill switch;
- same-symbol conflict policies: `allow_hedged`, `net_exposure`, `highest_confidence`, `highest_expected_value`, and `block_conflict`.

Risk decisions are logged as explainable events.

## Outputs

Portfolio runs write:

- `portfolio_config_resolved.yaml`
- `portfolio_summary.json`
- `strategy_contributions.csv`
- `portfolio_equity_curve.csv`
- `portfolio_drawdown_curve.csv`
- `portfolio_trades.csv`
- `portfolio_orders.csv`
- `portfolio_positions.csv`
- `risk_events.jsonl`
- `conflict_resolution_events.jsonl`
- `deployment_events.jsonl`

Use `strategy_contributions.csv` to see PnL, return, trade count, and drawdown by sleeve. Use `portfolio_summary.json` for combined portfolio performance.

## CLI

```bash
bt portfolio backtest --config configs/portfolios/equal_weight_demo.yaml
bt portfolio report --run-id outputs/portfolios/equal_weight_demo
bt portfolio demo --config configs/portfolios/equal_weight_demo.yaml
bt portfolio live --config configs/portfolios/live_template.yaml
```

## Live Safety

Live mode refuses to start unless the config explicitly contains:

```yaml
deployment:
  mode: live
  confirm_live_trading: true
```

Use `demo` or `paper` modes before enabling live trading.

