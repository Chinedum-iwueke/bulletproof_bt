"""SaaS application service for Strategy Robustness Lab V1."""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from bt.metrics.r_metrics import summarize_r
from bt.saas.models import (
    AnalysisCapabilityProfile,
    AnalysisRunConfig,
    DiagnosticCapability,
    EngineAnalysisResult,
    EngineRunContext,
    IngestedRun,
    ParsedArtifactInput,
    ScorePayload,
)


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
        metadata["equity_curve_provenance"] = "reconstructed_from_trades"
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
                equity_curve_provenance = "reconstructed_from_trades"
            else:
                equity_curve_provenance = "engine_emitted"
        else:
            equity = self._equity_from_trades(normalized)
            equity_curve_provenance = "reconstructed_from_trades"

        performance_path = root / "performance.json"
        if performance_path.exists():
            performance = json.loads(performance_path.read_text(encoding="utf-8"))
        else:
            performance = self._compute_performance_from_trades(normalized, equity)

        metadata = self._extract_metadata(normalized, strategy_name=root.name)
        metadata["run_dir"] = str(root)
        metadata["equity_curve_provenance"] = equity_curve_provenance

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

        overview = self._overview(
            run,
            score=asdict(score),
            monte_carlo=monte_carlo,
            risk_of_ruin=risk_of_ruin,
        )
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

    def run_analysis_from_parsed_artifact(
        self,
        parsed_artifact: ParsedArtifactInput,
        *,
        config: AnalysisRunConfig | None = None,
    ) -> EngineAnalysisResult:
        config = config or AnalysisRunConfig()
        run = self._ingested_run_from_parsed_artifact(parsed_artifact)
        payload = self.build_dashboard_payload(
            run,
            seed=config.seed,
            simulations=config.simulations,
            ruin_drawdown_levels=config.ruin_drawdown_levels,
            account_size=config.account_size,
            risk_per_trade_pct=config.risk_per_trade_pct,
        )

        diagnostics = {
            "overview": payload["overview"],
            "distribution": payload["trade_distribution"],
            "monte_carlo": payload["monte_carlo"],
            "stability": payload["parameter_stability"],
            "execution": payload["execution_sensitivity"],
            "regimes": payload["regime_analysis"],
            "ruin": payload["risk_of_ruin"],
            "report": payload["validation_report"],
        }

        capability_profile = AnalysisCapabilityProfile(
            diagnostics=self._diagnostic_capability_profile(parsed_artifact)
        )
        diagnostics = self._apply_diagnostic_eligibility(
            diagnostics=diagnostics,
            capability_profile=capability_profile,
            diagnostic_eligibility=parsed_artifact.diagnostic_eligibility,
        )
        diagnostics = {
            name: self._decorate_diagnostic_payload(
                name=name,
                payload=payload_block,
                capability=capability_profile.diagnostics[name],
            )
            if name in capability_profile.diagnostics
            else payload_block
            for name, payload_block in diagnostics.items()
        }
        warnings = list(payload["overview"].get("warnings", [])) + list(parsed_artifact.parser_notes)

        run_context = EngineRunContext(
            artifact_kind=parsed_artifact.artifact_kind,
            richness=parsed_artifact.richness,
            trade_count=len(parsed_artifact.trades),
            ohlcv_present=parsed_artifact.ohlcv_present,
            benchmark_present=parsed_artifact.benchmark_present,
            has_assumptions=parsed_artifact.assumptions is not None,
            has_params=parsed_artifact.params is not None,
        )
        return EngineAnalysisResult(
            run_context=run_context,
            capability_profile=capability_profile,
            warnings=warnings,
            diagnostics=diagnostics,
            raw_payload=payload,
        )

    def _ingested_run_from_parsed_artifact(self, parsed_artifact: ParsedArtifactInput) -> IngestedRun:
        if not parsed_artifact.trades:
            raise IngestionError("Parsed artifact must include at least one normalized trade record.")

        frame = pd.DataFrame(
            {
                "trade_id": trade.trade_id,
                "symbol": trade.symbol,
                "side": trade.side,
                "entry_time": trade.entry_time,
                "exit_time": trade.exit_time,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "quantity": trade.quantity,
                "fees": trade.fees,
                "pnl": trade.pnl,
                "mae_price": trade.mae,
                "mfe_price": trade.mfe,
                "strategy_name": trade.strategy_name,
                "timeframe": trade.timeframe,
                "market": trade.market,
                "exchange": trade.exchange,
            }
            for trade in parsed_artifact.trades
        )
        normalized = self._normalize_trades(frame)

        if parsed_artifact.equity_curve:
            equity = pd.DataFrame(parsed_artifact.equity_curve)
            if "timestamp" in equity.columns and "ts" not in equity.columns:
                equity = equity.rename(columns={"timestamp": "ts"})
            if "equity" not in equity.columns:
                equity = self._equity_from_trades(normalized)
                equity_curve_provenance = "reconstructed_from_trades"
            else:
                equity_curve_provenance = "engine_emitted"
        else:
            equity = self._equity_from_trades(normalized)
            equity_curve_provenance = "reconstructed_from_trades"

        performance = self._compute_performance_from_trades(normalized, equity)
        metadata = self._extract_metadata(
            normalized,
            strategy_name=str(parsed_artifact.strategy_metadata.get("strategy_name", "parsed_artifact")),
        )
        metadata.update(parsed_artifact.strategy_metadata)
        metadata.update(
            {
                "artifact_kind": parsed_artifact.artifact_kind,
                "richness": parsed_artifact.richness,
                "ohlcv_present": parsed_artifact.ohlcv_present,
                "benchmark_present": parsed_artifact.benchmark_present,
                "params_present": parsed_artifact.params is not None,
                "assumptions_present": parsed_artifact.assumptions is not None,
                "equity_curve_provenance": equity_curve_provenance,
            }
        )

        return IngestedRun(
            source="parsed_artifact",
            trades=normalized,
            equity=equity,
            performance=performance,
            metadata=metadata,
        )

    def _diagnostic_capability_profile(
        self,
        parsed_artifact: ParsedArtifactInput,
    ) -> dict[str, DiagnosticCapability]:
        trade_count = len(parsed_artifact.trades)
        has_trades = trade_count > 0
        has_params = parsed_artifact.params is not None
        def status_for_trade_based(name: str) -> DiagnosticCapability:
            if not has_trades:
                return DiagnosticCapability(
                    status="unavailable",
                    reason="No trades supplied in parsed artifact.",
                    required_inputs=["trades"],
                    optional_enrichments=["assumptions", "params", "ohlcv"],
                )
            if trade_count < 30:
                return DiagnosticCapability(
                    status="limited",
                    reason=f"{name} computed from fewer than 30 trades.",
                    required_inputs=["trades"],
                    optional_enrichments=["assumptions", "params", "ohlcv"],
                )
            return DiagnosticCapability(
                status="supported",
                reason=f"{name} can run from normalized trade history.",
                required_inputs=["trades"],
                optional_enrichments=["assumptions", "params", "ohlcv", "benchmark"],
            )

        stability = status_for_trade_based("stability")
        if has_trades and not has_params:
            stability = DiagnosticCapability(
                status="limited",
                reason="Using single-run stability proxy because params/grid context is missing.",
                required_inputs=["trades"],
                optional_enrichments=["params", "assumptions", "ohlcv"],
            )

        regimes = status_for_trade_based("regimes")
        if has_trades and not parsed_artifact.ohlcv_present:
            regimes = DiagnosticCapability(
                status="limited",
                reason="Regime analysis is trade-sequence based without OHLCV context.",
                required_inputs=["trades"],
                optional_enrichments=["ohlcv", "benchmark", "params"],
            )

        report_status = "supported" if has_trades else "unavailable"
        report_reason = (
            "Report synthesized from available diagnostics."
            if has_trades
            else "Cannot assemble report without diagnostics from trade data."
        )

        return {
            "overview": status_for_trade_based("overview"),
            "distribution": status_for_trade_based("distribution"),
            "monte_carlo": status_for_trade_based("monte_carlo"),
            "stability": stability,
            "execution": (
                DiagnosticCapability(
                    status="supported" if has_trades else "unavailable",
                    reason=(
                        "Execution sensitivity computed from fees/slippage/spread when present; defaults to zero costs otherwise."
                        if has_trades
                        else "No trades supplied in parsed artifact."
                    ),
                    required_inputs=["trades"],
                    optional_enrichments=["assumptions", "params"],
                )
            ),
            "regimes": regimes,
            "ruin": DiagnosticCapability(
                status="supported" if has_trades else "unavailable",
                reason=(
                    "Risk of ruin derived from Monte Carlo drawdown distribution."
                    if has_trades
                    else "No trades supplied in parsed artifact."
                ),
                required_inputs=["trades"],
                optional_enrichments=["account_size", "risk_per_trade_pct", "assumptions"],
            ),
            "report": DiagnosticCapability(
                status=report_status,
                reason=report_reason,
                required_inputs=["trades"],
                optional_enrichments=["assumptions", "params", "ohlcv", "benchmark"],
            ),
        }


    def _apply_diagnostic_eligibility(
        self,
        *,
        diagnostics: dict[str, dict[str, Any]],
        capability_profile: AnalysisCapabilityProfile,
        diagnostic_eligibility: dict[str, bool],
    ) -> dict[str, dict[str, Any]]:
        if not diagnostic_eligibility:
            return diagnostics

        filtered: dict[str, dict[str, Any]] = {}
        for name, payload in diagnostics.items():
            enabled = diagnostic_eligibility.get(name, True)
            if enabled:
                filtered[name] = payload
                continue

            capability = capability_profile.diagnostics.get(name)
            reason = "Skipped by upstream diagnostic_eligibility policy."
            if capability is not None and capability.status == "unavailable":
                reason = capability.reason

            filtered[name] = {
                "status": "skipped",
                "reason": reason,
            }

            if capability is not None:
                capability_profile.diagnostics[name] = DiagnosticCapability(
                    status="unavailable",
                    reason=reason,
                    required_inputs=capability.required_inputs,
                    optional_enrichments=capability.optional_enrichments,
                )

        return filtered

    def _unwrap_report_assumptions(self, payload: dict[str, Any]) -> list[str]:
        assumptions = payload.get("assumptions")
        if isinstance(assumptions, list):
            return [str(item) for item in assumptions]
        if isinstance(assumptions, dict):
            return [f"{key}: {value}" for key, value in assumptions.items()]
        return []

    def _decorate_diagnostic_payload(
        self,
        *,
        name: str,
        payload: dict[str, Any],
        capability: DiagnosticCapability,
    ) -> dict[str, Any]:
        if payload.get("status") == "skipped":
            return payload

        available = capability.status != "unavailable"
        limited = capability.status == "limited"
        reason_unavailable = None if available else capability.reason
        limitations = [capability.reason] if capability.reason else []
        assumptions = payload.get("assumptions", [])
        if name == "report":
            assumptions = self._unwrap_report_assumptions(payload)

        decorated = {
            "available": available,
            "limited": limited,
            "reason_unavailable": reason_unavailable,
            "limitations": limitations,
            "summary_metrics": payload.get("summary_metrics", {}),
            "figures": payload.get("figures", []),
            "interpretation": payload.get("interpretation", []),
            "warnings": payload.get("warnings", []),
            "assumptions": assumptions,
            "recommendations": payload.get("recommendations", []),
            "metadata": payload.get("metadata", {}),
            "payload": payload,
        }
        decorated.update(payload)
        return decorated

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

    def _figure_line_series(
        self,
        *,
        figure_id: str,
        title: str,
        x_label: str,
        y_label: str,
        x_values: list[Any],
        series: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "id": figure_id,
            "type": "line_series",
            "title": title,
            "x_label": x_label,
            "y_label": y_label,
            "x": x_values,
            "series": series,
        }

    def _figure_histogram(
        self,
        *,
        figure_id: str,
        title: str,
        x_label: str,
        y_label: str,
        bins: list[dict[str, float]],
    ) -> dict[str, Any]:
        return {
            "id": figure_id,
            "type": "histogram",
            "title": title,
            "x_label": x_label,
            "y_label": y_label,
            "bins": bins,
        }

    def _histogram_bins(self, values: list[float], *, bins: int = 12) -> list[dict[str, float]]:
        if not values:
            return []
        counts, edges = np.histogram(np.asarray(values, dtype=float), bins=min(max(bins, 3), 40))
        return [
            {
                "start": float(edges[idx]),
                "end": float(edges[idx + 1]),
                "count": float(counts[idx]),
            }
            for idx in range(len(counts))
        ]

    def _quantiles(self, values: np.ndarray, *, points: tuple[float, ...]) -> dict[str, float]:
        if values.size == 0:
            return {str(point): 0.0 for point in points}
        return {str(point): float(np.quantile(values, point)) for point in points}

    def _overview(
        self,
        run: IngestedRun,
        *,
        score: dict[str, Any],
        monte_carlo: dict[str, Any],
        risk_of_ruin: dict[str, Any],
    ) -> dict[str, Any]:
        equity_curve = self._equity_curve_payload(run.equity)
        figures: list[dict[str, Any]] = []
        if equity_curve:
            figures.append(
                self._figure_line_series(
                    figure_id="equity_curve",
                    title="Equity Curve",
                    x_label="timestamp",
                    y_label="equity",
                    x_values=[point["ts"] for point in equity_curve],
                    series=[{"name": "strategy_equity", "values": [point["equity"] for point in equity_curve]}],
                )
            )
        warnings = self._warnings(run.performance)
        total_trades = int(run.performance.get("total_trades", 0))
        max_drawdown_pct = float(run.performance.get("max_drawdown_pct", 0.0))
        expectancy = float(run.performance.get("ev_net", 0.0))
        win_rate = float(run.performance.get("win_rate", 0.0))
        profit_factor = run.performance.get("profit_factor")
        pnl = run.trades["pnl_net"].fillna(0.0)
        wins = pnl[pnl > 0]
        losses = pnl[pnl < 0]
        payoff_ratio = float(wins.mean() / abs(losses.mean())) if len(wins) and len(losses) and losses.mean() != 0 else None
        worst_mc_drawdown = float(monte_carlo.get("summary_metrics", {}).get("worst_simulated_drawdown_pct", 0.0))
        ruin_probability = risk_of_ruin.get("summary_metrics", {}).get("probability_of_ruin")

        posture, confidence, verdict_reasons = self._overview_verdict(
            score=score,
            trade_count=total_trades,
            max_drawdown_pct=max_drawdown_pct,
            expectancy=expectancy,
            win_rate=win_rate,
            profit_factor=profit_factor,
            worst_mc_drawdown=worst_mc_drawdown,
        )
        positives, cautions = self._overview_highlights(
            trade_count=total_trades,
            expectancy=expectancy,
            win_rate=win_rate,
            profit_factor=profit_factor,
            max_drawdown_pct=max_drawdown_pct,
            worst_mc_drawdown=worst_mc_drawdown,
        )
        figure_provenance = str(run.metadata.get("equity_curve_provenance", "reconstructed_from_trades"))
        limitations = [
            "No benchmark-relative context is included in overview yet.",
            "Parameter-topology metadata is absent, so overfitting diagnostics remain proxy-level.",
            "Execution and slippage assumptions are limited to trade-record fields.",
            "Regime context is trade-sequence only unless OHLCV context is supplied.",
        ]
        return {
            "summary_metrics": {
                "robustness_score": float(score.get("overall", 0.0)),
                "trade_count": total_trades,
                "win_rate": win_rate,
                "expectancy": expectancy,
                "profit_factor": float(profit_factor) if profit_factor is not None else None,
                "payoff_ratio": payoff_ratio,
                "realized_max_drawdown_pct": max_drawdown_pct,
                "worst_mc_drawdown_pct": worst_mc_drawdown,
                "ruin_probability": float(ruin_probability) if ruin_probability is not None else None,
                "overfitting_risk": float(max(0.0, min(1.0, 1.0 - float(score.get("sub_scores", {}).get("parameter_stability", 0.0)) / 100.0))),
                "posture": posture,
                "confidence": confidence,
            },
            "figures": figures,
            "interpretation": {
                "summary": (
                    f"Overview posture is {posture.replace('_', ' ')} with {confidence} confidence "
                    f"from {total_trades} realized trades and Monte Carlo stress."
                ),
                "positives": positives,
                "cautions": cautions,
            },
            "warnings": warnings,
            "assumptions": [
                "Overview is computed on normalized trade records and assumes uploaded trades are complete and chronologically accurate.",
                "Equity curve is reconstructed from trade-level net PnL whenever explicit equity points are not supplied.",
                "Execution realism is bounded by provided cost fields (fees/slippage/spread); missing fields imply zero-cost baseline.",
                "Monte Carlo survivability assumes bootstrap resampling with replacement from realized trade outcomes.",
            ],
            "limitations": limitations,
            "recommendations": [
                "Upload benchmark context once benchmark comparison is enabled to evaluate relative performance.",
                "Provide parameter metadata or experiment grid outputs for stronger overfitting and stability conclusions.",
                "Include explicit execution assumptions (latency/slippage model) to tighten risk posture confidence.",
                "Attach OHLCV context to unlock regime-aware decomposition beyond trade-sequence proxies.",
            ],
            "verdict": {
                "posture": posture,
                "confidence": confidence,
                "verdict_reasons": verdict_reasons,
            },
            "metadata": {
                "diagnostics_used": ["performance", "score", "equity_curve"],
                "figure_provenance": {
                    "equity_curve": figure_provenance,
                    "benchmark_overlay": "reserved_not_emitted",
                },
                "artifact_richness": str(run.metadata.get("richness", run.source)),
                "completeness_flags": {
                    "has_equity_curve_points": bool(equity_curve),
                    "benchmark_present": bool(run.metadata.get("benchmark_present", False)),
                    "params_present": bool(run.metadata.get("params_present", False)),
                    "ohlcv_present": bool(run.metadata.get("ohlcv_present", False)),
                },
            },
            "strategy": run.metadata,
            "headline_metrics": run.performance,
            "robustness_score": score["overall"],
            "sub_scores": score["sub_scores"],
            "equity_curve": equity_curve,
        }

    def _overview_verdict(
        self,
        *,
        score: dict[str, Any],
        trade_count: int,
        max_drawdown_pct: float,
        expectancy: float,
        win_rate: float,
        profit_factor: float | None,
        worst_mc_drawdown: float,
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []
        robustness_score = float(score.get("overall", 0.0))

        if trade_count < 10:
            posture = "inconclusive_due_to_missing_context"
            confidence = "low"
            reasons.append("Very small sample size (<10 trades) limits statistical reliability.")
        elif expectancy <= 0.0:
            posture = "fragile_under_stress"
            confidence = "medium" if trade_count >= 30 else "low"
            reasons.append("Non-positive expectancy weakens base edge.")
        elif max_drawdown_pct <= -35.0 or worst_mc_drawdown <= -45.0:
            posture = "fragile_under_stress"
            confidence = "medium"
            reasons.append("Drawdown stress indicates notable capital fragility.")
        elif robustness_score >= 70.0 and trade_count >= 50 and (profit_factor is None or float(profit_factor) >= 1.3):
            posture = "robust_under_current_assumptions"
            confidence = "high"
            reasons.append("Robustness score, trade count, and efficiency metrics are strong under current assumptions.")
        elif robustness_score >= 55.0 and trade_count >= 20 and win_rate >= 0.45:
            posture = "promising_but_incomplete"
            confidence = "medium"
            reasons.append("Core metrics are constructive, but supporting context is incomplete.")
        else:
            posture = "inconclusive_due_to_missing_context"
            confidence = "low"
            reasons.append("Signal quality is mixed or thin relative to required conviction.")

        reasons.append(f"Observed trade count: {trade_count}.")
        reasons.append(f"Realized max drawdown: {max_drawdown_pct:.2f}%.")
        reasons.append(f"Worst Monte Carlo drawdown: {worst_mc_drawdown:.2f}%.")
        return posture, confidence, reasons

    def _overview_highlights(
        self,
        *,
        trade_count: int,
        expectancy: float,
        win_rate: float,
        profit_factor: float | None,
        max_drawdown_pct: float,
        worst_mc_drawdown: float,
    ) -> tuple[list[str], list[str]]:
        positives: list[str] = []
        cautions: list[str] = []
        if expectancy > 0.0:
            positives.append(f"Positive expectancy per trade ({expectancy:.4f}).")
        else:
            cautions.append(f"Expectancy is non-positive ({expectancy:.4f}).")
        if profit_factor is not None and float(profit_factor) > 1.0:
            positives.append(f"Profit factor above 1.0 ({float(profit_factor):.2f}).")
        else:
            cautions.append("Profit factor is missing or not above 1.0.")
        if win_rate >= 0.5:
            positives.append(f"Win rate is at/above 50% ({win_rate:.1%}).")
        else:
            cautions.append(f"Win rate below 50% ({win_rate:.1%}).")
        if trade_count < 30:
            cautions.append(f"Low sample size ({trade_count} trades) reduces confidence.")
        if max_drawdown_pct <= -25.0:
            cautions.append(f"Realized drawdown is material ({max_drawdown_pct:.2f}%).")
        if worst_mc_drawdown <= -35.0:
            cautions.append(f"Monte Carlo worst drawdown is severe ({worst_mc_drawdown:.2f}%).")
        return positives, cautions

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
        pnl_values = [float(value) for value in pnl.tolist()]
        trade_count = int(len(pnl))

        durations: list[float] = []
        if "exit_ts" in trades.columns:
            exit_ts = pd.to_datetime(trades["exit_ts"], utc=True, errors="coerce")
            duration_series = (exit_ts - trades["entry_ts"]).dt.total_seconds() / 60.0
            durations = [
                float(value)
                for value in duration_series.dropna().tolist()
                if np.isfinite(value) and value >= 0.0
            ]

        mae_mfe_points = [
            {"x": float(mae), "y": float(mfe), "label": str(trade_id)}
            for mae, mfe, trade_id in zip(
                trades["mae_price"].fillna(np.nan),
                trades["mfe_price"].fillna(np.nan),
                trades.index,
                strict=True,
            )
            if not np.isnan(mae) and not np.isnan(mfe)
        ]
        has_mae_mfe = bool(mae_mfe_points)
        has_duration = bool(durations)

        r_values = [float(value) for value in trades["r_multiple_net"].dropna().tolist()]
        r_summary = summarize_r(r_values)
        wins = pnl[pnl > 0]
        losses = pnl[pnl < 0]
        win_count = int((pnl > 0).sum())
        loss_count = int((pnl < 0).sum())
        flat_count = int((pnl == 0).sum())
        gross_profit = float(wins.sum())
        gross_loss = float(losses.sum())
        gross_loss_abs = float(abs(losses.sum()))
        payoff_ratio = float(wins.mean() / abs(losses.mean())) if len(wins) and len(losses) and losses.mean() != 0 else None
        profit_factor = float(gross_profit / gross_loss_abs) if gross_loss_abs > 0 else None
        win_rate = float((pnl > 0).mean()) if len(pnl) else 0.0
        mean_return = float(pnl.mean()) if len(pnl) else 0.0
        median_return = float(pnl.median()) if len(pnl) else 0.0
        return_std = float(pnl.std(ddof=1)) if trade_count > 1 else 0.0
        skewness = float(pnl.skew()) if trade_count > 2 else 0.0
        kurtosis = float(pnl.kurt()) if trade_count > 3 else 0.0
        percentile_10 = float(np.quantile(np.asarray(pnl_values, dtype=float), 0.10)) if trade_count else 0.0
        percentile_90 = float(np.quantile(np.asarray(pnl_values, dtype=float), 0.90)) if trade_count else 0.0

        right_tail_share = (
            float(wins[wins >= wins.quantile(0.9)].sum() / gross_profit)
            if len(wins) >= 2 and gross_profit > 0
            else None
        )
        loss_tail_share = (
            float(abs(losses[losses <= losses.quantile(0.2)].sum()) / gross_loss_abs)
            if len(losses) >= 2 and gross_loss_abs > 0
            else None
        )

        shape_insights: list[dict[str, Any]] = []
        skew_direction = "right_skewed" if skewness > 0.25 else ("left_skewed" if skewness < -0.25 else "approximately_symmetric")
        shape_insights.append(
            {
                "type": "skew",
                "signal": skew_direction,
                "value": skewness,
            }
        )
        tail_signal = "balanced_tails"
        if right_tail_share is not None and right_tail_share >= 0.5:
            tail_signal = "right_tail_dependent_payoff_profile"
        elif loss_tail_share is not None and loss_tail_share >= 0.6:
            tail_signal = "loss_concentration_risk"
        shape_insights.append(
            {
                "type": "tail_concentration",
                "signal": tail_signal,
                "right_tail_profit_share": right_tail_share,
                "loss_tail_share": loss_tail_share,
            }
        )
        if win_rate < 0.5 and payoff_ratio is not None and payoff_ratio > 1.0:
            shape_insights.append(
                {
                    "type": "outcome_asymmetry",
                    "signal": "many_small_losses_fewer_larger_wins",
                    "value": payoff_ratio,
                }
            )
        elif win_rate >= 0.5 and (payoff_ratio is None or payoff_ratio <= 1.0):
            shape_insights.append(
                {
                    "type": "outcome_asymmetry",
                    "signal": "high_hit_rate_low_payoff_profile",
                    "value": payoff_ratio,
                }
            )
        elif abs(mean_return) < max(1e-12, 0.10 * return_std):
            shape_insights.append(
                {
                    "type": "outcome_asymmetry",
                    "signal": "symmetric_but_weak_expectancy",
                    "value": mean_return,
                }
            )

        positives: list[str] = []
        cautions: list[str] = []
        if mean_return > 0.0:
            positives.append(f"Positive expectancy per trade ({mean_return:.4f}).")
        else:
            cautions.append(f"Expectancy is non-positive ({mean_return:.4f}).")
        if profit_factor is not None and profit_factor > 1.0:
            positives.append(f"Profit factor is above 1.0 ({profit_factor:.2f}).")
        else:
            cautions.append("Profit factor is undefined or not above 1.0.")
        if trade_count < 30:
            cautions.append(f"Distribution confidence is limited by sample size ({trade_count} trades).")
        if return_std == 0.0 and trade_count > 1:
            cautions.append("Trade outcomes have near-zero dispersion; stress assumptions may be understated.")

        summary = (
            "Trade outcomes show "
            f"{skew_direction.replace('_', ' ')}, "
            f"expectancy {mean_return:.4f}, "
            f"and win rate {win_rate:.1%} across {trade_count} trades."
        )

        limitations = [
            "No regime-conditioned distribution (requires OHLCV/regime labels).",
            "No parameter-conditioned distribution (requires parameter/grid metadata).",
        ]
        if not has_mae_mfe:
            limitations.append("No MAE/MFE excursion data in normalized trades.")
        if not has_duration:
            limitations.append("No usable exit timestamps to derive duration distribution.")

        recommendations = [
            "Include OHLCV or regime labels to unlock regime-conditioned distribution diagnostics.",
            "Include parameter metadata or grid outputs for parameter-segmented distribution analysis.",
        ]
        if not has_mae_mfe:
            recommendations.append("Upload MAE/MFE excursion fields to unlock excursion scatter diagnostics.")
        if not has_duration:
            recommendations.append("Include exit timestamps or explicit duration fields for duration distribution analysis.")

        figures: list[dict[str, Any]] = [
            self._figure_histogram(
                figure_id="trade_return_histogram",
                title="Trade Return Distribution",
                x_label="pnl_net",
                y_label="trade_count",
                bins=self._histogram_bins(pnl_values, bins=min(max(int(np.sqrt(max(trade_count, 1))) + 6, 8), 24)),
            ),
            {
                "id": "win_loss_distribution",
                "type": "bar_groups",
                "title": "Win/Loss Distribution",
                "x_label": "outcome",
                "y_label": "trade_count",
                "groups": [
                    {"label": "wins", "count": win_count, "pct": float(win_count / trade_count) if trade_count else 0.0},
                    {"label": "losses", "count": loss_count, "pct": float(loss_count / trade_count) if trade_count else 0.0},
                    {"label": "flat", "count": flat_count, "pct": float(flat_count / trade_count) if trade_count else 0.0},
                ],
            },
        ]
        figures[0]["metadata"] = {
            "value_field": "pnl_net",
            "trade_count": trade_count,
            "bin_count": len(figures[0]["bins"]),
            "mean_marker": mean_return,
            "median_marker": median_return,
            "percentile_10_marker": percentile_10,
            "percentile_90_marker": percentile_90,
        }

        if has_mae_mfe:
            figures.append(
                {
                    "id": "mae_mfe_scatter",
                    "type": "scatter",
                    "title": "MAE/MFE Scatter",
                    "x_label": "mae_price",
                    "y_label": "mfe_price",
                    "points": mae_mfe_points,
                }
            )
        if has_duration:
            figures.append(
                self._figure_histogram(
                    figure_id="duration_histogram",
                    title="Trade Duration Distribution",
                    x_label="duration_minutes",
                    y_label="trade_count",
                    bins=self._histogram_bins(durations, bins=min(max(int(np.sqrt(len(durations))) + 4, 6), 20)),
                )
            )

        warnings = []
        if profit_factor is None or payoff_ratio is None:
            warnings.append("Profit factor and payoff ratio require both winning and losing trades.")
        if not has_mae_mfe:
            warnings.append("Excursion diagnostics omitted because MAE/MFE fields are missing.")
        if not has_duration:
            warnings.append("Duration diagnostics omitted because exit timestamps are missing or invalid.")

        return {
            "summary_metrics": {
                "trade_count": trade_count,
                "expectancy": mean_return,
                "win_rate": win_rate,
                "mean_return": mean_return,
                "median_return": median_return,
                "return_std": return_std,
                "gross_profit": gross_profit,
                "gross_loss": gross_loss,
                "gross_loss_abs": gross_loss_abs,
                "profit_factor": profit_factor,
                "payoff_ratio": payoff_ratio,
                "mean_duration": float(np.mean(durations)) if durations else None,
                "median_duration": float(np.median(durations)) if durations else None,
                "avg_duration_minutes": float(np.mean(durations)) if durations else None,
                "median_duration_minutes": float(np.median(durations)) if durations else None,
                "percentile_10": percentile_10,
                "percentile_90": percentile_90,
                "skewness": skewness,
                "kurtosis": kurtosis,
            },
            "figures": figures,
            "interpretation": {
                "summary": summary,
                "positives": positives,
                "cautions": cautions,
                "shape_insights": shape_insights,
            },
            "warnings": warnings,
            "assumptions": [
                "Distribution metrics are derived from normalized per-trade pnl_net values.",
                "Duration statistics are emitted only when valid entry and exit timestamps are available.",
                "Excursion diagnostics are emitted only when both MAE and MFE are present on trades.",
            ],
            "limitations": limitations,
            "recommendations": recommendations,
            "metadata": {
                "trade_count": trade_count,
                "coverage_window": {
                    "start": trades["entry_ts"].min().isoformat() if trade_count else None,
                    "end": trades["entry_ts"].max().isoformat() if trade_count else None,
                },
                "available_subdiagnostics": {
                    "histogram_available": True,
                    "win_loss_available": True,
                    "mae_mfe_available": has_mae_mfe,
                    "duration_available": has_duration,
                },
                "completeness_notes": limitations,
                "has_durations": has_duration,
                "has_mae_mfe": has_mae_mfe,
            },
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
                "summary_metrics": {
                    "worst_simulated_drawdown_pct": 0.0,
                    "median_drawdown_pct": 0.0,
                    "drawdown_p95_pct": 0.0,
                    "probability_of_ruin": 0.0,
                },
                "figures": [],
                "interpretation": ["Monte Carlo unavailable because trade outcomes or simulations are missing."],
                "warnings": ["No Monte Carlo simulations were produced."],
                "assumptions": ["Bootstrap simulation requires at least one trade return and simulations > 0."],
                "recommendations": ["Provide trade outcomes and simulation count > 0 to enable Monte Carlo diagnostics."],
                "metadata": {"simulations": 0, "trades_in_bootstrap": int(pnl.size)},
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

        quantiles = self._quantiles(max_drawdowns, points=(0.5, 0.95))
        percentile_paths = np.quantile(equity_paths, [0.1, 0.5, 0.9], axis=0)

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
            "summary_metrics": {
                "worst_simulated_drawdown_pct": float(max_drawdowns.min() * 100.0),
                "median_drawdown_pct": float(quantiles["0.5"] * 100.0),
                "drawdown_p95_pct": float(quantiles["0.95"] * 100.0),
                "probability_of_ruin": probability_of_ruin,
                "ruin_threshold_equity": ruin_threshold_equity,
            },
            "figures": [
                {
                    "id": "equity_fan_chart",
                    "type": "fan_chart",
                    "title": "Monte Carlo Equity Fan",
                    "x_label": "trade_index",
                    "y_label": "equity",
                    "x": list(range(1, pnl.size + 1)),
                    "bands": {
                        "p10": [float(value) for value in percentile_paths[0].tolist()],
                        "p50": [float(value) for value in percentile_paths[1].tolist()],
                        "p90": [float(value) for value in percentile_paths[2].tolist()],
                    },
                },
                self._figure_histogram(
                    figure_id="drawdown_histogram",
                    title="Simulated Max Drawdown Distribution",
                    x_label="max_drawdown_pct",
                    y_label="simulation_count",
                    bins=self._histogram_bins([float(value * 100.0) for value in max_drawdowns.tolist()], bins=16),
                ),
            ],
            "interpretation": [
                "Monte Carlo uses bootstrap resampling of realized trade outcomes with replacement.",
                "Ruin probability is estimated as the fraction of paths crossing the ruin equity threshold.",
            ],
            "warnings": [],
            "assumptions": [
                "Trade outcomes are IID under bootstrap resampling.",
                "No serial correlation/regime conditioning is modeled in this baseline Monte Carlo.",
            ],
            "recommendations": [
                "Provide richer regime labels or OHLCV context for conditional Monte Carlo in future revisions.",
            ],
            "metadata": {"trades_in_bootstrap": int(pnl.size), "simulations": int(simulations)},
        }

    def _parameter_stability_from_single_run(self, performance: dict[str, Any]) -> dict[str, Any]:
        ev = float(performance.get("ev_net", 0.0))
        score = 65.0 if ev > 0 else 35.0
        return {
            "summary_metrics": {
                "stability_score": score,
                "plateau_ratio": None,
                "peak_fragility": None,
            },
            "figures": [],
            "interpretation": [
                "Only a single-run proxy is available because parameter-grid metadata is absent."
            ],
            "warnings": ["Full stability topology requires experiment grid metadata."],
            "assumptions": ["Proxy score is directional and should not be interpreted as topology evidence."],
            "recommendations": ["Upload experiment grid summary to unlock heatmap/surface stability diagnostics."],
            "metadata": {"mode": "single_run_proxy"},
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
                "summary_metrics": {
                    "stability_score": 0.0,
                    "plateau_ratio": 0.0,
                    "peak_fragility": 1.0,
                },
                "figures": [],
                "interpretation": ["Parameter stability grid was supplied but metric values are empty."],
                "warnings": ["Could not compute stability from empty metric series."],
                "assumptions": [],
                "recommendations": ["Ensure selected metric column is numeric and populated in grid summary."],
                "metadata": {"grid_points": 0},
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
            "summary_metrics": {
                "stability_score": score,
                "plateau_ratio": plateau_ratio,
                "peak_fragility": fragility,
            },
            "figures": [
                {
                    "id": "stability_heatmap",
                    "type": "heatmap",
                    "title": "Parameter Stability Heatmap",
                    "x_label": x_key,
                    "y_label": y_key,
                    "value_label": "metric",
                    "cells": heatmap,
                }
            ],
            "interpretation": ["Lower fragility and wider plateau imply more parameter robustness."],
            "warnings": [],
            "assumptions": ["Stability is derived from the selected grid metric topology."],
            "recommendations": ["Validate top plateau parameters out-of-sample before deployment."],
            "metadata": {"grid_points": int(len(metric_series)), "axes": {"x": x_key, "y": y_key, "value": "metric"}},
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
            "summary_metrics": {
                "baseline_ev_net": float(base.mean()) if len(base) else 0.0,
                "break_even_cost_multiplier": break_even_multiplier,
                "execution_resilience_score": resilience,
            },
            "figures": [
                self._figure_line_series(
                    figure_id="cost_sensitivity_curve",
                    title="Execution Cost Sensitivity",
                    x_label="cost_multiplier",
                    y_label="ev_net",
                    x_values=[point["multiplier"] for point in blended_curve],
                    series=[
                        {"name": "fees_only", "values": [point["ev_net"] for point in fee_curve]},
                        {"name": "slippage_spread", "values": [point["ev_net"] for point in slippage_curve]},
                        {"name": "combined_costs", "values": [point["ev_net"] for point in blended_curve]},
                    ],
                )
            ],
            "interpretation": [
                "Execution diagnostic stresses fees/slippage/spread against realized per-trade outcomes."
            ],
            "warnings": [],
            "assumptions": [
                "Stress multipliers apply proportionally to observed costs and do not model liquidity feedback."
            ],
            "recommendations": ["Collect venue-level slippage assumptions for richer execution what-if scenarios."],
            "metadata": {"multipliers": multipliers},
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
            "summary_metrics": {
                "regime_consistency_score": consistency,
            },
            "figures": [
                {
                    "id": "session_expectancy_bars",
                    "type": "bar_groups",
                    "title": "Session Expectancy",
                    "x_label": "session",
                    "y_label": "mean_pnl_net",
                    "groups": [{"label": str(k), "value": float(v)} for k, v in by_session.items()],
                },
                {
                    "id": "volatility_regime_bars",
                    "type": "bar_groups",
                    "title": "Volatility Regime Expectancy",
                    "x_label": "volatility_regime",
                    "y_label": "mean_pnl_net",
                    "groups": [{"label": str(k), "value": float(v)} for k, v in vol_expectancy.items()],
                },
            ],
            "interpretation": [
                "Regime analysis here is inferred from trade-sequence proxies unless explicit OHLCV regime labels are supplied."
            ],
            "warnings": [],
            "assumptions": [
                "Volatility and trend regimes are proxied from rolling trade PnL statistics in trade-only mode."
            ],
            "recommendations": ["Upload OHLCV/regime labels for genuine market-regime decomposition."],
            "metadata": {"proxy_mode": True},
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
            "summary_metrics": {
                "probability_of_ruin": float(monte_carlo.get("probability_of_ruin", 0.0)),
                "expected_worst_drawdown_pct": expected_worst_drawdown,
                "capital_threshold": float(monte_carlo.get("ruin_threshold_equity", account_size * 0.5)),
            },
            "figures": [
                {
                    "id": "ruin_threshold_curve",
                    "type": "line_series",
                    "title": "Ruin Probability by Drawdown Threshold",
                    "x_label": "drawdown_threshold",
                    "y_label": "probability",
                    "x": list(levels.keys()),
                    "series": [{"name": "probability", "values": [float(value) for value in levels.values()]}],
                }
            ],
            "interpretation": [
                "Ruin metrics are tied to Monte Carlo survivability under the configured ruin equity threshold."
            ],
            "warnings": [],
            "assumptions": [
                "Ruin threshold defaults to 50% of account equity if not overridden by account policy."
            ],
            "recommendations": ["Set explicit account_size and risk_per_trade_pct for tighter ruin-context outputs."],
            "metadata": {"drawdown_levels": list(levels.keys())},
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
        available_diagnostics = {
            "overview": True,
            "distribution": True,
            "monte_carlo": bool(monte_carlo.get("simulations", 0) > 0),
            "stability": parameter_stability.get("status") != "single_run_only",
            "execution": True,
            "regimes": True,
            "ruin": True,
        }
        return {
            "summary_metrics": {
                "robustness_score": float(score["overall"]),
                "trade_count": int(run.performance.get("total_trades", 0)),
                "available_diagnostic_count": int(sum(1 for enabled in available_diagnostics.values() if enabled)),
            },
            "figures": [],
            "interpretation": [
                f"Final posture: {interpretation}.",
                "Report summarizes only diagnostics that were truthfully computable from supplied inputs.",
            ],
            "warnings": [],
            "assumptions": [
                "Report sections are synthesized from deterministic diagnostics using configured Monte Carlo seed."
            ],
            "recommendations": [
                "Provide richer artifact bundles (benchmark/OHLCV/parameter grid) to unlock deeper diagnostics."
            ],
            "metadata": {
                "available_diagnostics": available_diagnostics,
                "export_sections": [
                    "executive_summary",
                    "validation_posture",
                    "limitations",
                    "recommendations",
                ],
            },
            "header": {
                "strategy_name": run.metadata.get("strategy_name"),
                "date_start": run.metadata.get("date_start"),
                "date_end": run.metadata.get("date_end"),
                "source": run.source,
            },
            "executive_summary": {
                "verdict": interpretation,
                "robustness_score": float(score["overall"]),
                "top_risks": [
                    "Monte Carlo drawdown profile",
                    "Execution cost sensitivity",
                ],
            },
            "validation_posture": {
                "deterministic": True,
                "seed": int(seed),
                "simulations": int(simulations),
            },
            "limitations": [
                "Trade-only artifacts cannot fully support parameter-topology stability or true market-regime decomposition."
            ],
            "strategy_summary": run.metadata,
            "assumptions_detail": {
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


def run_analysis_from_parsed_artifact(
    parsed_artifact: ParsedArtifactInput,
    *,
    config: AnalysisRunConfig | None = None,
) -> EngineAnalysisResult:
    """Central engine seam for normalized parsed-artifact analysis."""
    service = StrategyRobustnessLabService()
    return service.run_analysis_from_parsed_artifact(parsed_artifact, config=config)
