"""SaaS application service for Strategy Robustness Lab V1."""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from bt.metrics.r_metrics import summarize_r
from bt.saas.models import IngestedRun, ScorePayload


REQUIRED_BASE_COLUMNS = {"entry_ts", "symbol", "side"}
SIDE_MAP = {"BUY": 1.0, "LONG": 1.0, "SELL": -1.0, "SHORT": -1.0}


class IngestionError(ValueError):
    """Raised when uploaded artifacts do not satisfy the V1 ingestion contract."""


class StrategyRobustnessLabService:
    """Builds deterministic, UI-ready robustness diagnostics payloads."""

    def ingest_trade_log(
        self,
        trade_csv_path: str | Path,
        *,
        strategy_name: str = "uploaded_strategy",
        initial_equity: float = 100_000.0,
    ) -> IngestedRun:
        trades = pd.read_csv(trade_csv_path)
        normalized = self._normalize_trades(trades)
        equity = self._equity_from_trades(normalized, initial_equity=initial_equity)
        performance = self._compute_performance_from_trades(normalized, equity)
        metadata = self._extract_metadata(normalized, strategy_name=strategy_name)
        return IngestedRun(
            source="trade_log",
            trades=normalized,
            equity=equity,
            performance=performance,
            metadata=metadata,
        )

    def ingest_run_artifacts(self, run_dir: str | Path) -> IngestedRun:
        root = Path(run_dir)
        trades_path = root / "trades.csv"
        if not trades_path.exists():
            raise IngestionError(f"Missing required artifact: {trades_path}")

        normalized = self._normalize_trades(pd.read_csv(trades_path))

        equity_path = root / "equity.csv"
        if equity_path.exists():
            equity = pd.read_csv(equity_path)
            if "ts" not in equity.columns:
                if "timestamp" in equity.columns:
                    equity = equity.rename(columns={"timestamp": "ts"})
                else:
                    equity = equity.assign(ts=normalized["entry_ts"])
            if "equity" not in equity.columns:
                equity = self._equity_from_trades(normalized)
        else:
            equity = self._equity_from_trades(normalized)

        performance_path = root / "performance.json"
        if performance_path.exists():
            performance = json.loads(performance_path.read_text(encoding="utf-8"))
        else:
            performance = self._compute_performance_from_trades(normalized, equity)

        metadata = self._extract_metadata(normalized, strategy_name=root.name)
        metadata["run_dir"] = str(root)

        return IngestedRun(
            source="run_artifacts",
            trades=normalized,
            equity=equity,
            performance=performance,
            metadata=metadata,
        )

    def build_dashboard_payload(
        self,
        run: IngestedRun,
        *,
        seed: int = 42,
        simulations: int = 1_000,
        ruin_drawdown_levels: tuple[float, ...] = (0.30, 0.50),
        account_size: float | None = None,
        risk_per_trade_pct: float | None = None,
    ) -> dict[str, Any]:
        equity_start = float(
            account_size
            if account_size is not None
            else run.performance.get("initial_equity", 100_000.0)
        )

        monte_carlo = self._monte_carlo(
            trades=run.trades,
            seed=seed,
            simulations=simulations,
            initial_equity=equity_start,
            drawdown_levels=ruin_drawdown_levels,
        )
        parameter_stability = self._parameter_stability_from_single_run(run.performance)
        execution_sensitivity = self._execution_sensitivity(run.trades)
        regime = self._regime_analysis(run.trades)
        risk_of_ruin = self._risk_of_ruin(
            monte_carlo,
            account_size=equity_start,
            risk_per_trade_pct=risk_per_trade_pct,
        )
        score = self._score(
            performance=run.performance,
            monte_carlo=monte_carlo,
            parameter_stability=parameter_stability,
            execution_sensitivity=execution_sensitivity,
            regime=regime,
        )

        overview = self._overview(run, asdict(score))
        trade_distribution = self._trade_distribution(run.trades)
        report = self._validation_report(
            run=run,
            monte_carlo=monte_carlo,
            parameter_stability=parameter_stability,
            execution_sensitivity=execution_sensitivity,
            regime=regime,
            risk_of_ruin=risk_of_ruin,
            score=asdict(score),
            seed=seed,
            simulations=simulations,
        )

        return {
            "overview": overview,
            "trade_distribution": trade_distribution,
            "monte_carlo": monte_carlo,
            "parameter_stability": parameter_stability,
            "execution_sensitivity": execution_sensitivity,
            "regime_analysis": regime,
            "risk_of_ruin": risk_of_ruin,
            "score": asdict(score),
            "validation_report": report,
        }

    def parameter_stability_from_grid(
        self,
        grid_summary_csv: str | Path,
        *,
        metric: str = "ev_net",
    ) -> dict[str, Any]:
        frame = pd.read_csv(grid_summary_csv)
        parameter_columns = [column for column in frame.columns if column.startswith("strategy.")]
        if len(parameter_columns) < 2:
            raise IngestionError(
                "Grid summary must include at least two strategy.* parameter columns"
            )
        if metric not in frame.columns:
            raise IngestionError(f"Grid summary missing metric column '{metric}'")

        x_key, y_key = parameter_columns[:2]
        heatmap = (
            frame[[x_key, y_key, metric]]
            .dropna()
            .rename(columns={x_key: "x", y_key: "y", metric: "value"})
            .to_dict(orient="records")
        )
        return self._parameter_stability_common(
            metric_series=pd.to_numeric(frame[metric], errors="coerce").dropna(),
            heatmap=heatmap,
            x_key=x_key,
            y_key=y_key,
        )

    def _normalize_trades(self, trades: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            "timestamp": "entry_ts",
            "entry_time": "entry_ts",
            "entry_timestamp": "entry_ts",
            "exit_time": "exit_ts",
            "exit_timestamp": "exit_ts",
            "direction": "side",
            "qty": "quantity",
            "size": "quantity",
            "fee": "fees_paid",
            "fees": "fees_paid",
            "commission": "fees_paid",
            "pnl": "pnl_net",
            "risk": "risk_amount",
        }
        normalized = trades.rename(
            columns={key: value for key, value in rename_map.items() if key in trades.columns}
        ).copy()

        missing_base = sorted(REQUIRED_BASE_COLUMNS - set(normalized.columns))
        if missing_base:
            raise IngestionError(
                f"Trade log missing required columns {missing_base}. "
                "Required minimum: entry timestamp, symbol, direction."
            )

        if "quantity" not in normalized.columns and "pnl_net" not in normalized.columns:
            raise IngestionError(
                "Trade log requires either quantity/size or pnl/pnl_net so trade outcomes can be evaluated."
            )

        normalized["entry_ts"] = pd.to_datetime(normalized["entry_ts"], utc=True, errors="coerce")
        if normalized["entry_ts"].isna().any():
            raise IngestionError("entry_ts contains invalid timestamps; use ISO-8601 timestamps.")

        if "exit_ts" in normalized.columns:
            normalized["exit_ts"] = pd.to_datetime(normalized["exit_ts"], utc=True, errors="coerce")

        numeric_defaults = {
            "entry_price": np.nan,
            "exit_price": np.nan,
            "quantity": np.nan,
            "fees_paid": 0.0,
            "pnl_net": np.nan,
            "pnl_price": np.nan,
            "slippage": 0.0,
            "spread": 0.0,
            "risk_amount": np.nan,
            "mae_price": np.nan,
            "mfe_price": np.nan,
            "r_multiple_net": np.nan,
            "r_multiple_gross": np.nan,
        }
        for column, default in numeric_defaults.items():
            if column not in normalized.columns:
                normalized[column] = default
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        if (
            normalized["pnl_net"].isna().all()
            and normalized["entry_price"].notna().all()
            and normalized["exit_price"].notna().all()
            and normalized["quantity"].notna().all()
        ):
            direction = (
                normalized["side"].astype(str).str.upper().map(SIDE_MAP).fillna(1.0)
            )
            gross = (
                (normalized["exit_price"] - normalized["entry_price"])
                * normalized["quantity"]
                * direction
            )
            normalized["pnl_price"] = gross
            normalized["pnl_net"] = gross - normalized["fees_paid"].fillna(0.0)

        if normalized["pnl_net"].isna().any():
            raise IngestionError(
                "Could not infer pnl_net for all trades. Provide pnl/pnl_net, or provide "
                "entry_price, exit_price, quantity, and side for every row."
            )

        if normalized["r_multiple_net"].isna().all() and normalized["risk_amount"].notna().any():
            risk = normalized["risk_amount"].replace(0.0, np.nan)
            normalized["r_multiple_net"] = normalized["pnl_net"] / risk

        if "symbol" in normalized.columns:
            normalized["symbol"] = normalized["symbol"].astype(str)

        return normalized.sort_values("entry_ts").reset_index(drop=True)

    def _equity_from_trades(
        self,
        trades: pd.DataFrame,
        *,
        initial_equity: float = 100_000.0,
    ) -> pd.DataFrame:
        pnl = trades["pnl_net"].fillna(0.0)
        equity = float(initial_equity) + pnl.cumsum()
        return pd.DataFrame({"ts": trades["entry_ts"], "equity": equity})

    def _compute_performance_from_trades(
        self,
        trades: pd.DataFrame,
        equity: pd.DataFrame,
    ) -> dict[str, Any]:
        pnl = trades["pnl_net"].fillna(0.0)
        wins = pnl > 0

        peak = equity["equity"].cummax()
        drawdown = (equity["equity"] / peak) - 1.0
        downside = pnl[pnl < 0]
        return {
            "total_trades": int(len(trades)),
            "ev_net": float(pnl.mean()) if len(pnl) else 0.0,
            "win_rate": float(wins.mean()) if len(pnl) else 0.0,
            "max_drawdown_pct": float(drawdown.min() * 100.0) if len(drawdown) else 0.0,
            "final_equity": float(equity["equity"].iloc[-1]) if not equity.empty else 0.0,
            "initial_equity": float(equity["equity"].iloc[0] - pnl.iloc[0]) if len(pnl) else 100_000.0,
            "profit_factor": float(pnl[pnl > 0].sum() / abs(downside.sum())) if len(downside) and abs(downside.sum()) > 0 else None,
        }

    def _extract_metadata(self, trades: pd.DataFrame, *, strategy_name: str) -> dict[str, Any]:
        return {
            "strategy_name": strategy_name,
            "symbols": sorted(set(trades["symbol"].astype(str))),
            "date_start": trades["entry_ts"].min().isoformat() if not trades.empty else None,
            "date_end": trades["entry_ts"].max().isoformat() if not trades.empty else None,
            "trade_count": int(len(trades)),
        }

    def _overview(self, run: IngestedRun, score: dict[str, Any]) -> dict[str, Any]:
        return {
            "strategy": run.metadata,
            "headline_metrics": run.performance,
            "robustness_score": score["overall"],
            "sub_scores": score["sub_scores"],
            "warnings": self._warnings(run.performance),
            "equity_curve": self._equity_curve_payload(run.equity),
        }

    def _equity_curve_payload(self, equity: pd.DataFrame) -> list[dict[str, Any]]:
        frame = equity.copy()
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["ts", "equity"])
        return [
            {"ts": ts.isoformat(), "equity": float(value)}
            for ts, value in zip(frame["ts"], frame["equity"], strict=True)
        ]

    def _warnings(self, performance: dict[str, Any]) -> list[str]:
        warnings: list[str] = []
        if int(performance.get("total_trades", 0)) < 30:
            warnings.append("Low sample size: fewer than 30 trades.")
        if float(performance.get("max_drawdown_pct", 0.0)) <= -30.0:
            warnings.append("Historical max drawdown exceeded 30%.")
        if float(performance.get("ev_net", 0.0)) <= 0.0:
            warnings.append("Non-positive expectancy detected.")
        return warnings

    def _trade_distribution(self, trades: pd.DataFrame) -> dict[str, Any]:
        pnl = trades["pnl_net"].fillna(0.0)
        durations = []
        if "exit_ts" in trades.columns:
            exit_ts = pd.to_datetime(trades["exit_ts"], utc=True, errors="coerce")
            duration_series = (exit_ts - trades["entry_ts"]).dt.total_seconds() / 60.0
            durations = [float(value) for value in duration_series.dropna().tolist()]

        r_values = [float(value) for value in trades["r_multiple_net"].dropna().tolist()]
        r_summary = summarize_r(r_values)

        return {
            "r_multiple_distribution": r_values,
            "r_multiple_summary": {
                "count": r_summary.n,
                "ev_r": r_summary.ev_r,
                "win_rate_r": r_summary.win_rate,
                "profit_factor_r": r_summary.profit_factor_r,
            },
            "mae_distribution": [float(value) for value in trades["mae_price"].dropna().tolist()],
            "mfe_distribution": [float(value) for value in trades["mfe_price"].dropna().tolist()],
            "duration_minutes_distribution": durations,
            "streak_distribution": self._streak_distribution(pnl),
        }

    def _streak_distribution(self, pnl: pd.Series) -> dict[str, list[int]]:
        signs = pnl.apply(lambda value: 1 if value > 0 else (-1 if value < 0 else 0)).tolist()
        win_streaks: list[int] = []
        loss_streaks: list[int] = []
        current_sign = 0
        current_length = 0

        for sign in signs:
            if sign == 0:
                continue
            if sign == current_sign:
                current_length += 1
                continue
            if current_sign > 0:
                win_streaks.append(current_length)
            elif current_sign < 0:
                loss_streaks.append(current_length)
            current_sign = sign
            current_length = 1

        if current_sign > 0:
            win_streaks.append(current_length)
        elif current_sign < 0:
            loss_streaks.append(current_length)

        return {"wins": win_streaks, "losses": loss_streaks}

    def _monte_carlo(
        self,
        *,
        trades: pd.DataFrame,
        seed: int,
        simulations: int,
        initial_equity: float,
        drawdown_levels: tuple[float, ...],
        fan_chart_paths: int = 50,
    ) -> dict[str, Any]:
        pnl = trades["pnl_net"].fillna(0.0).to_numpy(dtype=float)
        if pnl.size == 0 or simulations <= 0:
            return {
                "methodology": {
                    "method": "bootstrap_trade_returns",
                    "replacement": True,
                    "simulations": 0,
                    "seed": seed,
                },
                "simulations": 0,
                "fan_chart_paths": [],
                "drawdown_distribution_pct": [],
                "worst_drawdown_pct": 0.0,
                "median_drawdown_pct": 0.0,
                "probability_by_drawdown_threshold": {},
                "probability_of_ruin": 0.0,
                "ruin_threshold_equity": float(initial_equity * 0.5),
            }

        rng = np.random.default_rng(seed)
        sampled = rng.choice(pnl, size=(simulations, pnl.size), replace=True)

        equity_paths = float(initial_equity) + np.cumsum(sampled, axis=1)
        running_peaks = np.maximum.accumulate(equity_paths, axis=1)
        drawdowns = np.where(running_peaks > 0, (equity_paths - running_peaks) / running_peaks, 0.0)
        max_drawdowns = drawdowns.min(axis=1)

        threshold_probs = {
            f"dd_{int(level * 100)}": float((max_drawdowns <= -float(level)).mean())
            for level in drawdown_levels
        }

        ruin_threshold_equity = float(initial_equity * 0.5)
        probability_of_ruin = float((equity_paths.min(axis=1) <= ruin_threshold_equity).mean())

        return {
            "methodology": {
                "method": "bootstrap_trade_returns",
                "replacement": True,
                "simulations": int(simulations),
                "seed": int(seed),
            },
            "simulations": int(simulations),
            "fan_chart_paths": [
                [float(value) for value in row]
                for row in equity_paths[: min(fan_chart_paths, simulations)].tolist()
            ],
            "drawdown_distribution_pct": [float(value * 100.0) for value in max_drawdowns.tolist()],
            "worst_drawdown_pct": float(max_drawdowns.min() * 100.0),
            "median_drawdown_pct": float(np.median(max_drawdowns) * 100.0),
            "probability_by_drawdown_threshold": threshold_probs,
            "probability_of_ruin": probability_of_ruin,
            "ruin_threshold_equity": ruin_threshold_equity,
        }

    def _parameter_stability_from_single_run(self, performance: dict[str, Any]) -> dict[str, Any]:
        ev = float(performance.get("ev_net", 0.0))
        score = 65.0 if ev > 0 else 35.0
        return {
            "stability_score": score,
            "plateau_ratio": None,
            "peak_fragility": None,
            "heatmap": [],
            "status": "single_run_only",
            "interpretation": "Upload experiment grid summary for full parameter stability diagnostics.",
        }

    def _parameter_stability_common(
        self,
        *,
        metric_series: pd.Series,
        heatmap: list[dict[str, Any]],
        x_key: str,
        y_key: str,
    ) -> dict[str, Any]:
        if metric_series.empty:
            return {
                "stability_score": 0.0,
                "plateau_ratio": 0.0,
                "peak_fragility": 1.0,
                "heatmap": heatmap,
                "axes": {"x": x_key, "y": y_key, "value": "metric"},
            }

        top_quantile = float(metric_series.quantile(0.90))
        plateau_ratio = float((metric_series >= top_quantile).mean())
        peak = float(metric_series.max())
        median = float(metric_series.median())
        fragility = 1.0 if peak == 0 else max(0.0, min(1.0, 1.0 - (median / peak)))
        score = max(0.0, min(100.0, (plateau_ratio * 60.0) + ((1.0 - fragility) * 40.0)))
        return {
            "stability_score": score,
            "plateau_ratio": plateau_ratio,
            "peak_fragility": fragility,
            "heatmap": heatmap,
            "axes": {"x": x_key, "y": y_key, "value": "metric"},
        }

    def _execution_sensitivity(self, trades: pd.DataFrame) -> dict[str, Any]:
        base = trades["pnl_net"].fillna(0.0)
        fees = trades["fees_paid"].fillna(0.0)
        slippage = trades["slippage"].fillna(0.0)
        spread = trades["spread"].fillna(0.0)

        multipliers = [1.0, 1.25, 1.5, 2.0, 3.0]
        fee_curve = []
        slippage_curve = []
        blended_curve = []
        for multiplier in multipliers:
            fee_stressed = base - ((multiplier - 1.0) * fees)
            slip_stressed = base - ((multiplier - 1.0) * (slippage + spread))
            blended_stressed = base - ((multiplier - 1.0) * (fees + slippage + spread))
            fee_curve.append({"multiplier": multiplier, "ev_net": float(fee_stressed.mean())})
            slippage_curve.append({"multiplier": multiplier, "ev_net": float(slip_stressed.mean())})
            blended_curve.append({"multiplier": multiplier, "ev_net": float(blended_stressed.mean())})

        break_even_multiplier = next(
            (point["multiplier"] for point in blended_curve if point["ev_net"] <= 0.0),
            None,
        )
        resilience = 100.0 if break_even_multiplier is None else max(
            0.0,
            min(100.0, (float(break_even_multiplier) / 3.0) * 100.0),
        )
        return {
            "baseline_ev_net": float(base.mean()) if len(base) else 0.0,
            "fee_curve": fee_curve,
            "slippage_spread_curve": slippage_curve,
            "combined_cost_curve": blended_curve,
            "break_even_cost_multiplier": break_even_multiplier,
            "execution_resilience_score": resilience,
        }

    def _regime_analysis(self, trades: pd.DataFrame) -> dict[str, Any]:
        pnl = trades["pnl_net"].fillna(0.0)
        entry_hour = trades["entry_ts"].dt.hour

        session_bucket = pd.cut(
            entry_hour,
            bins=[-1, 7, 15, 23],
            labels=["asia", "europe", "us"],
        )
        by_session = (
            pd.DataFrame({"session": session_bucket, "pnl": pnl})
            .groupby("session", observed=False)["pnl"]
            .mean()
            .fillna(0.0)
            .to_dict()
        )

        rolling_vol = pnl.rolling(20, min_periods=5).std().fillna(0.0)
        if len(trades) >= 6:
            vol_rank = rolling_vol.rank(method="first")
            vol_regime = pd.qcut(vol_rank, q=3, labels=["low", "mid", "high"])
        else:
            vol_regime = pd.Series(["mid"] * len(trades), index=trades.index)
        vol_expectancy = (
            pd.DataFrame({"vol_regime": vol_regime, "pnl": pnl})
            .groupby("vol_regime", observed=False)["pnl"]
            .mean()
            .to_dict()
        )

        trend = pnl.rolling(10, min_periods=4).mean().fillna(0.0)
        trend_regime = np.where(trend >= 0.0, "trend", "range")
        trend_expectancy = (
            pd.DataFrame({"trend_regime": trend_regime, "pnl": pnl})
            .groupby("trend_regime")["pnl"]
            .mean()
            .to_dict()
        )

        dispersion = float(np.std(list(vol_expectancy.values()))) if vol_expectancy else 0.0
        consistency = max(0.0, min(100.0, 100.0 - (dispersion * 100.0)))

        return {
            "volatility_regime_expectancy": {str(k): float(v) for k, v in vol_expectancy.items()},
            "trend_range_expectancy": {str(k): float(v) for k, v in trend_expectancy.items()},
            "session_expectancy": {str(k): float(v) for k, v in by_session.items()},
            "regime_consistency_score": consistency,
        }

    def _risk_of_ruin(
        self,
        monte_carlo: dict[str, Any],
        *,
        account_size: float,
        risk_per_trade_pct: float | None,
    ) -> dict[str, Any]:
        levels = monte_carlo.get("probability_by_drawdown_threshold", {})
        expected_worst_drawdown = float(monte_carlo.get("worst_drawdown_pct", 0.0))

        projected_risk_capital = None
        if risk_per_trade_pct is not None:
            projected_risk_capital = float(account_size) * float(risk_per_trade_pct)

        return {
            "probability_of_ruin": float(monte_carlo.get("probability_of_ruin", 0.0)),
            "probability_drawdown_30": float(levels.get("dd_30", 0.0)),
            "probability_drawdown_50": float(levels.get("dd_50", 0.0)),
            "expected_worst_drawdown_pct": expected_worst_drawdown,
            "capital_threshold": float(monte_carlo.get("ruin_threshold_equity", account_size * 0.5)),
            "account_size": float(account_size),
            "risk_per_trade_pct": risk_per_trade_pct,
            "projected_risk_capital_per_trade": projected_risk_capital,
        }

    def _score(
        self,
        *,
        performance: dict[str, Any],
        monte_carlo: dict[str, Any],
        parameter_stability: dict[str, Any],
        execution_sensitivity: dict[str, Any],
        regime: dict[str, Any],
    ) -> ScorePayload:
        win_rate = float(performance.get("win_rate", 0.0))
        profit_factor = performance.get("profit_factor")
        profit_factor_component = 50.0
        if profit_factor is not None:
            profit_factor_component = max(0.0, min(100.0, (float(profit_factor) / 2.0) * 100.0))

        statistical_quality = max(0.0, min(100.0, (win_rate * 60.0) + (profit_factor_component * 0.4)))
        drawdown_resilience = max(0.0, min(100.0, 100.0 + min(0.0, float(performance.get("max_drawdown_pct", 0.0)))))
        monte_carlo_stability = max(0.0, min(100.0, 100.0 - abs(float(monte_carlo.get("median_drawdown_pct", 0.0)))))
        execution_resilience = float(execution_sensitivity.get("execution_resilience_score", 0.0))
        parameter_score = float(parameter_stability.get("stability_score", 0.0))
        regime_consistency = float(regime.get("regime_consistency_score", 0.0))

        overall = (
            0.25 * statistical_quality
            + 0.25 * monte_carlo_stability
            + 0.20 * drawdown_resilience
            + 0.15 * execution_resilience
            + 0.15 * parameter_score
        )

        methodology = {
            "weights": {
                "statistical_quality": 0.25,
                "monte_carlo_stability": 0.25,
                "drawdown_resilience": 0.20,
                "execution_resilience": 0.15,
                "parameter_stability": 0.15,
            },
            "scale": "0_to_100",
            "note": "Regime consistency is reported as a supporting sub-score and can be promoted to weighted V1.1.",
        }

        return ScorePayload(
            overall=float(max(0.0, min(100.0, overall))),
            sub_scores={
                "statistical_quality": statistical_quality,
                "monte_carlo_stability": monte_carlo_stability,
                "drawdown_resilience": drawdown_resilience,
                "execution_resilience": execution_resilience,
                "parameter_stability": parameter_score,
                "regime_consistency": regime_consistency,
            },
            methodology=methodology,
        )

    def _validation_report(
        self,
        *,
        run: IngestedRun,
        monte_carlo: dict[str, Any],
        parameter_stability: dict[str, Any],
        execution_sensitivity: dict[str, Any],
        regime: dict[str, Any],
        risk_of_ruin: dict[str, Any],
        score: dict[str, Any],
        seed: int,
        simulations: int,
    ) -> dict[str, Any]:
        interpretation = "Robust candidate" if score["overall"] >= 60.0 else "Deploy with caution"
        return {
            "strategy_summary": run.metadata,
            "assumptions": {
                "ingestion_source": run.source,
                "monte_carlo_seed": seed,
                "monte_carlo_simulations": simulations,
                "deterministic": True,
            },
            "performance_summary": run.performance,
            "monte_carlo_diagnostics": monte_carlo,
            "parameter_stability": parameter_stability,
            "execution_sensitivity": execution_sensitivity,
            "regime_analysis": regime,
            "risk_of_ruin": risk_of_ruin,
            "score": score,
            "final_verdict": {
                "robustness_score": score["overall"],
                "interpretation": interpretation,
            },
        }
