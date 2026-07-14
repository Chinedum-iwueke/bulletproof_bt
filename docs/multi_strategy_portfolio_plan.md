# Multi-Strategy Portfolio Allocation Plan

## Architecture Inspection Summary

- Engine entrypoint: `bt.api.run_backtest` builds one `BacktestEngine` per run.
- Core loop: `BacktestEngine` owns one strategy, one risk engine, one execution model, and one portfolio.
- Strategy config: existing configs use `strategy.name` plus strategy parameters.
- Hypothesis config: `HypothesisContract` materializes one hypothesis grid into independent run specs.
- Risk sizing: `RiskEngine.signal_to_order_intent` sizes a single signal against one shared account snapshot.
- Ledger/output schemas: existing trades/fills/decisions already support identity fields, but portfolio-level artifacts do not exist.
- Demo/live code: execution services currently wrap single portfolio/runtime primitives.
- V1 scope: `README.md` explicitly excludes multi-strategy blending and portfolio allocation engines.

## Implementation Plan

1. Add a new `bt.portfolio_engine` package instead of changing the single-strategy engine contract.
2. Introduce typed dataclasses for portfolio identifiers, allocation config, run state, tagged signals/orders/trades, and equity curves.
3. Implement allocation policy resolution for fixed weight, equal weight, simple risk parity, vol target, and manual capital buckets.
4. Implement a portfolio risk coordinator that audits exposures, per-strategy buckets, daily loss/drawdown kill switches, and same-symbol conflicts.
5. Implement a portfolio backtest runner that runs enabled strategy sleeves with isolated capital through the existing proven engine, tags/aggregates artifacts, and writes portfolio-level reports.
6. Add paper/demo and live-readiness runners that reuse the same portfolio config and risk coordinator; live mode must require `deployment.confirm_live_trading: true`.
7. Add CLI subcommands under `bt portfolio ...`.
8. Add docs, example configs, and focused tests for allocation, risk gating, artifacts, demo, live safety, and backward compatibility.

This first slice intentionally preserves all existing single-strategy behavior and gives the portfolio layer its own public surface for future tighter same-clock execution.
