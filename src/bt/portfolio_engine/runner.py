"""Portfolio backtest orchestration and artifact aggregation."""
from __future__ import annotations

import csv
import json
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bt.api import run_backtest
from bt.logging.formatting import write_json_deterministic
from bt.portfolio_engine.allocation import resolve_capital_buckets, resolve_strategy_weights
from bt.portfolio_engine.config import load_portfolio_config
from bt.portfolio_engine.models import PortfolioConfig, StrategyAllocationConfig
from bt.portfolio_engine.risk import PortfolioRiskCoordinator


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _safe_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def _portfolio_config_to_dict(config: PortfolioConfig) -> dict[str, Any]:
    payload = asdict(config)
    for strategy in payload["strategies"]:
        for key in ("config_path", "data_path"):
            if strategy.get(key) is not None:
                strategy[key] = str(strategy[key])
    if payload.get("data_path") is not None:
        payload["data_path"] = str(payload["data_path"])
    payload["output_dir"] = str(payload["output_dir"])
    return payload


def _strategy_override(
    *,
    portfolio: PortfolioConfig,
    strategy: StrategyAllocationConfig,
    capital: float,
) -> dict[str, Any]:
    override = {
        "initial_cash": float(capital),
        "identity": {
            "portfolio_id": portfolio.portfolio_id,
            "strategy_id": strategy.strategy_id,
            "hypothesis_id": strategy.hypothesis_id,
        },
    }
    if strategy.overrides:
        from bt.config import deep_merge

        override = deep_merge(override, strategy.overrides)
    return override


def _run_strategy_sleeve(
    *,
    portfolio: PortfolioConfig,
    strategy: StrategyAllocationConfig,
    capital: float,
    run_dir: Path,
    default_data_path: Path | None,
) -> Path:
    if strategy.config_path is None:
        raise ValueError(f"strategy {strategy.strategy_id} missing config_path")
    data_path = strategy.data_path or default_data_path
    if data_path is None:
        raise ValueError(f"strategy {strategy.strategy_id} missing data_path and portfolio.data_path is unset")

    override = _strategy_override(portfolio=portfolio, strategy=strategy, capital=capital)
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as handle:
        yaml.safe_dump(override, handle, sort_keys=False)
        override_path = Path(handle.name)
    try:
        return Path(
            run_backtest(
                config_path=str(strategy.config_path),
                data_path=str(data_path),
                out_dir=str(run_dir / "strategy_runs"),
                override_paths=[str(override_path)],
                run_name=f"{strategy.strategy_id}",
            )
        )
    finally:
        override_path.unlink(missing_ok=True)


def _tag_frame(frame: pd.DataFrame, *, portfolio: PortfolioConfig, strategy: StrategyAllocationConfig) -> pd.DataFrame:
    tagged = frame.copy()
    for column, value in (
        ("portfolio_id", portfolio.portfolio_id),
        ("strategy_id", strategy.strategy_id),
        ("hypothesis_id", strategy.hypothesis_id),
    ):
        if column in tagged.columns:
            tagged[column] = value
        else:
            tagged.insert(min(len(tagged.columns), 0 if column == "portfolio_id" else 1), column, value)
    return tagged


def _build_portfolio_equity(equity_frames: list[pd.DataFrame], *, starting_equity: float) -> pd.DataFrame:
    if not equity_frames:
        return pd.DataFrame(columns=["ts", "equity"])
    normalized: list[pd.DataFrame] = []
    for frame in equity_frames:
        if frame.empty or "ts" not in frame or "equity" not in frame:
            continue
        normalized.append(frame[["ts", "strategy_id", "equity"]].copy())
    if not normalized:
        return pd.DataFrame(columns=["ts", "equity"])
    all_equity = pd.concat(normalized, ignore_index=True)
    pivot = all_equity.pivot_table(index="ts", columns="strategy_id", values="equity", aggfunc="last").sort_index()
    pivot = pivot.ffill()
    result = pd.DataFrame({"ts": pivot.index.astype(str), "equity": pivot.sum(axis=1).values})
    if result.empty:
        return pd.DataFrame([{"ts": "", "equity": starting_equity}])
    return result


def _write_drawdown_curve(equity: pd.DataFrame, path: Path) -> None:
    if equity.empty or "equity" not in equity:
        pd.DataFrame(columns=["ts", "equity", "drawdown", "drawdown_pct"]).to_csv(path, index=False)
        return
    curve = equity.copy()
    high_watermark = curve["equity"].cummax()
    curve["drawdown"] = curve["equity"] - high_watermark
    curve["drawdown_pct"] = curve["drawdown"] / high_watermark.replace(0, pd.NA)
    curve.to_csv(path, index=False)


def _profit_factor(trades: pd.DataFrame) -> float | None:
    if trades.empty or "pnl" not in trades:
        return None
    wins = trades.loc[trades["pnl"] > 0, "pnl"].sum()
    losses = abs(trades.loc[trades["pnl"] < 0, "pnl"].sum())
    if losses == 0:
        return None if wins == 0 else float("inf")
    return float(wins / losses)


def _summary(
    *,
    portfolio: PortfolioConfig,
    weights: dict[str, float],
    capitals: dict[str, float],
    trades: pd.DataFrame,
    equity: pd.DataFrame,
    strategy_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    final_equity = float(equity["equity"].iloc[-1]) if not equity.empty else portfolio.starting_equity
    total_return = (final_equity / portfolio.starting_equity - 1.0) if portfolio.starting_equity else 0.0
    drawdown = 0.0
    if not equity.empty:
        hwm = equity["equity"].cummax()
        dd = (equity["equity"] - hwm) / hwm.replace(0, pd.NA)
        drawdown = float(dd.min() or 0.0)
    pnl = float(trades["pnl"].sum()) if not trades.empty and "pnl" in trades else 0.0
    return {
        "portfolio_id": portfolio.portfolio_id,
        "base_currency": portfolio.base_currency,
        "starting_equity": portfolio.starting_equity,
        "final_equity": final_equity,
        "total_return": total_return,
        "max_drawdown": drawdown,
        "pnl": pnl,
        "trade_count": int(len(trades)),
        "win_rate": float((trades["pnl"] > 0).mean()) if not trades.empty and "pnl" in trades else 0.0,
        "profit_factor": _profit_factor(trades),
        "allocation_weights": weights,
        "capital_buckets": capitals,
        "strategy_count": len(strategy_rows),
        "strategy_run_dirs": {row["strategy_id"]: row.get("run_dir") for row in strategy_rows},
    }


def run_portfolio_backtest(config: PortfolioConfig | str | Path) -> Path:
    portfolio = load_portfolio_config(config) if not isinstance(config, PortfolioConfig) else config
    run_dir = Path(portfolio.output_dir) / portfolio.portfolio_id
    run_dir.mkdir(parents=True, exist_ok=True)

    weights = resolve_strategy_weights(portfolio)
    capitals = resolve_capital_buckets(portfolio)
    coordinator = PortfolioRiskCoordinator(portfolio)

    _write_yaml(run_dir / "portfolio_config_resolved.yaml", _portfolio_config_to_dict(portfolio))

    tagged_trades: list[pd.DataFrame] = []
    tagged_orders: list[pd.DataFrame] = []
    tagged_equity: list[pd.DataFrame] = []
    contribution_rows: list[dict[str, Any]] = []

    for strategy in portfolio.enabled_strategies:
        capital = capitals[strategy.strategy_id]
        sleeve_run_dir = _run_strategy_sleeve(
            portfolio=portfolio,
            strategy=strategy,
            capital=capital,
            run_dir=run_dir,
            default_data_path=portfolio.data_path,
        )

        trades = _tag_frame(_safe_read_csv(sleeve_run_dir / "trades.csv"), portfolio=portfolio, strategy=strategy)
        if not trades.empty:
            tagged_trades.append(trades)

        equity = _tag_frame(_safe_read_csv(sleeve_run_dir / "equity.csv"), portfolio=portfolio, strategy=strategy)
        if not equity.empty:
            tagged_equity.append(equity)

        orders_rows: list[dict[str, Any]] = []
        decisions_path = sleeve_run_dir / "decisions.jsonl"
        if decisions_path.exists():
            for line in decisions_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                order = row.get("order") if isinstance(row.get("order"), dict) else {}
                orders_rows.append(
                    {
                        "portfolio_id": portfolio.portfolio_id,
                        "strategy_id": strategy.strategy_id,
                        "hypothesis_id": strategy.hypothesis_id,
                        "symbol": row.get("symbol"),
                        "timestamp": row.get("ts"),
                        "side": order.get("side"),
                        "qty": order.get("qty"),
                        "notional": row.get("notional_est"),
                        "approved": row.get("approved"),
                        "risk_rejection_reason": None if row.get("approved") else row.get("reason"),
                    }
                )
        if orders_rows:
            tagged_orders.append(pd.DataFrame(orders_rows))

        perf = _safe_json(sleeve_run_dir / "performance.json")
        contribution_rows.append(
            {
                "portfolio_id": portfolio.portfolio_id,
                "strategy_id": strategy.strategy_id,
                "hypothesis_id": strategy.hypothesis_id,
                "weight": weights[strategy.strategy_id],
                "allocated_capital": capital,
                "run_dir": str(sleeve_run_dir),
                "pnl_net": perf.get("net_pnl", 0.0),
                "total_return": (float(perf.get("net_pnl", 0.0)) / capital) if capital else 0.0,
                "trade_count": perf.get("trades", 0),
                "win_rate": perf.get("win_rate", 0.0),
                "profit_factor": perf.get("profit_factor"),
                "max_drawdown": perf.get("max_drawdown"),
            }
        )

    all_trades = pd.concat(tagged_trades, ignore_index=True) if tagged_trades else pd.DataFrame()
    order_columns = [
        "portfolio_id",
        "strategy_id",
        "hypothesis_id",
        "symbol",
        "timestamp",
        "side",
        "qty",
        "notional",
        "approved",
        "risk_rejection_reason",
    ]
    all_orders = pd.concat(tagged_orders, ignore_index=True) if tagged_orders else pd.DataFrame(columns=order_columns)
    all_equity_by_strategy = pd.concat(tagged_equity, ignore_index=True) if tagged_equity else pd.DataFrame()
    portfolio_equity = _build_portfolio_equity(tagged_equity, starting_equity=portfolio.starting_equity)

    all_trades.to_csv(run_dir / "portfolio_trades.csv", index=False)
    all_orders.to_csv(run_dir / "portfolio_orders.csv", index=False)
    all_equity_by_strategy.to_csv(run_dir / "strategy_equity_curves.csv", index=False)
    portfolio_equity.to_csv(run_dir / "portfolio_equity_curve.csv", index=False)
    _write_drawdown_curve(portfolio_equity, run_dir / "portfolio_drawdown_curve.csv")
    pd.DataFrame(contribution_rows).to_csv(run_dir / "strategy_contributions.csv", index=False)

    # Reserved schemas for richer same-clock execution. Write headers now so
    # downstream tooling can rely on stable artifact presence.
    pd.DataFrame(columns=["portfolio_id", "strategy_id", "hypothesis_id", "symbol", "side", "qty", "notional"]).to_csv(
        run_dir / "portfolio_positions.csv", index=False
    )
    for name, rows in {
        "risk_events.jsonl": coordinator.risk_events,
        "conflict_resolution_events.jsonl": coordinator.conflict_events,
        "deployment_events.jsonl": [],
    }.items():
        with (run_dir / name).open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")

    write_json_deterministic(
        run_dir / "portfolio_summary.json",
        _summary(
            portfolio=portfolio,
            weights=weights,
            capitals=capitals,
            trades=all_trades,
            equity=portfolio_equity,
            strategy_rows=contribution_rows,
        ),
    )
    return run_dir


def report_portfolio_run(run_dir: str | Path) -> dict[str, Any]:
    path = Path(run_dir) / "portfolio_summary.json"
    if not path.exists():
        raise FileNotFoundError(f"portfolio_summary.json not found under {run_dir}")
    return json.loads(path.read_text(encoding="utf-8"))
