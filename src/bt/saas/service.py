"""SaaS application service for Strategy Robustness Lab V1."""
from __future__ import annotations

import json
import hashlib
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from bt.analysis.overview.benchmark_overview import build_benchmark_overview_payload_from_metadata
from bt.metrics.r_metrics import summarize_r
from bt.saas.models import (
    AnalysisCapabilityProfile,
    AnalysisRunConfig,
    CAPABILITY_PROFILE_VERSION,
    DIAGNOSTIC_CONTRACT_VERSION,
    DiagnosticCapability,
    ENGINE_ADAPTER_VERSION,
    ENGINE_PARSER_VERSION,
    ENGINE_SEAM_NAME,
    ENGINE_SEAM_VERSION,
    EngineAnalysisResult,
    EngineEnvelopeV1,
    EngineRunContext,
    IngestedRun,
    NormalizedTradeRecord,
    ParameterSweepInput,
    ParameterSweepRunInput,
    ParsedArtifactInput,
    ScorePayload,
    STRATEGY_TRUTH_ROOM_CONTRACT_VERSION,
    STRATEGY_TRUTH_ROOM_VERDICTS,
    STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION,
    STRATEGY_RESEARCH_TERMINAL_CARD_SCHEMA_VERSION,
    StrategyTerminalBundle,
    StrategyTerminalCard,
)


REQUIRED_BASE_COLUMNS = {"entry_ts", "symbol", "side"}
SIDE_MAP = {"BUY": 1.0, "LONG": 1.0, "SELL": -1.0, "SHORT": -1.0}
_PARAM_VALUE_TYPES = (str, int, float, bool)


class IngestionError(ValueError):
    """Raised when uploaded artifacts do not satisfy the V1 ingestion contract."""


class StrategyRobustnessLabService:
    """Builds deterministic, UI-ready robustness diagnostics payloads."""

    def load_strategy_terminal_bundle(self, cards_json_path: str | Path) -> StrategyTerminalBundle:
        """Load Strategy Research Terminal cards through a stable SaaS contract."""
        path = Path(cards_json_path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        schema_version = str(payload.get("schema_version", ""))
        card_schema_version = str(payload.get("card_schema_version", ""))
        if schema_version != STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION:
            raise IngestionError(
                f"Unsupported terminal card bundle schema {schema_version!r}; "
                f"expected {STRATEGY_RESEARCH_TERMINAL_BUNDLE_SCHEMA_VERSION!r}."
            )
        if card_schema_version != STRATEGY_RESEARCH_TERMINAL_CARD_SCHEMA_VERSION:
            raise IngestionError(
                f"Unsupported terminal card schema {card_schema_version!r}; "
                f"expected {STRATEGY_RESEARCH_TERMINAL_CARD_SCHEMA_VERSION!r}."
            )

        cards: list[StrategyTerminalCard] = []
        for raw in payload.get("cards", []):
            if not isinstance(raw, dict):
                raise IngestionError("Terminal card bundle contains a non-object card.")
            if raw.get("schema_version") != STRATEGY_RESEARCH_TERMINAL_CARD_SCHEMA_VERSION:
                raise IngestionError(f"Unsupported terminal card schema in card {raw.get('card_type')!r}.")
            cards.append(
                StrategyTerminalCard(
                    schema_version=str(raw["schema_version"]),
                    card_type=str(raw["card_type"]),
                    card_id=str(raw["card_id"]),
                    name=str(raw["name"]),
                    phase=str(raw["phase"]),
                    hypothesis_name=str(raw["hypothesis_name"]),
                    pipeline_run_id=raw.get("pipeline_run_id"),
                    created_at=str(raw["created_at"]),
                    source_artifacts=dict(raw.get("source_artifacts") or {}),
                    data=dict(raw.get("data") or {}),
                    warnings=list(raw.get("warnings") or []),
                )
            )
        return StrategyTerminalBundle(
            schema_version=schema_version,
            card_schema_version=card_schema_version,
            created_at=str(payload.get("created_at", "")),
            cards=cards,
        )

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

    def ingest_parameter_sweep_bundle(
        self,
        bundle_dir: str | Path,
        *,
        metric: str = "ev_net",
    ) -> ParsedArtifactInput:
        root = Path(bundle_dir)
        manifest_path = root / "manifest.json"
        if not manifest_path.exists():
            raise IngestionError(f"Missing required parameter sweep manifest: {manifest_path}")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        parameter_names = manifest.get("parameter_names")
        if not isinstance(parameter_names, list) or not parameter_names or not all(
            isinstance(name, str) and name.strip() for name in parameter_names
        ):
            raise IngestionError("parameter_sweep manifest requires non-empty 'parameter_names' list[str].")
        canonical_parameter_names = [name.strip() for name in parameter_names]
        if len(set(canonical_parameter_names)) != len(canonical_parameter_names):
            raise IngestionError("parameter_sweep manifest parameter_names must be unique.")

        run_specs = manifest.get("runs")
        if not isinstance(run_specs, list) or len(run_specs) < 2:
            raise IngestionError("parameter_sweep manifest must contain at least two runs.")

        runs: list[ParameterSweepRunInput] = []
        flattened_trades: list[NormalizedTradeRecord] = []
        combinations: set[tuple[tuple[str, int | float | str | bool], ...]] = set()

        for spec in run_specs:
            run = self._parse_parameter_sweep_run_spec(
                root=root,
                run_spec=spec,
                parameter_names=canonical_parameter_names,
                metric=metric,
            )
            key = tuple((name, run.params[name]) for name in canonical_parameter_names)
            combinations.add(key)
            runs.append(run)
            flattened_trades.extend(run.trades)

        if len(combinations) < 2:
            raise IngestionError("parameter_sweep requires multiple unique parameter combinations.")

        strategy_name = str(manifest.get("strategy_name", root.name or "parameter_sweep"))
        return ParsedArtifactInput(
            artifact_kind="parameter_sweep",
            richness="research_complete",
            trades=flattened_trades,
            strategy_metadata={"strategy_name": strategy_name, "ingestion_contract": "parameter_sweep_bundle"},
            assumptions=manifest.get("assumptions") if isinstance(manifest.get("assumptions"), dict) else None,
            params={"parameter_names": canonical_parameter_names},
            parameter_sweep=ParameterSweepInput(
                parameter_names=canonical_parameter_names,
                runs=runs,
                assumptions=manifest.get("assumptions") if isinstance(manifest.get("assumptions"), dict) else None,
                execution_context=manifest.get("execution_context")
                if isinstance(manifest.get("execution_context"), dict)
                else None,
            ),
            parser_notes=["Parsed structured parameter sweep bundle via manifest.json contract."],
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
        semantic_capabilities = self._semantic_trade_capabilities(
            trades=run.trades,
            ohlcv_present=bool(run.metadata.get("ohlcv_present", False) or run.metadata.get("_ohlcv_context") is not None),
            benchmark_present=bool(run.metadata.get("benchmark_present", False)),
            params_present=bool(run.metadata.get("params_present", False)),
            parameter_sweep_present=False,
        )
        semantic_capabilities["has_broker_export"] = bool(run.metadata.get("broker_export_present", False))
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
            semantic_capabilities=semantic_capabilities,
        )
        parameter_stability = self._parameter_stability_from_single_run(run.performance)
        execution_sensitivity = self._execution_sensitivity(run.trades, semantic_capabilities=semantic_capabilities)
        regime = self._regime_analysis(run.trades, ohlcv=run.metadata.get("_ohlcv_context"))
        risk_of_ruin = self._risk_of_ruin(
            monte_carlo,
            trades=run.trades,
            account_size=equity_start,
            explicit_account_size=account_size,
            risk_per_trade_pct=risk_per_trade_pct,
            semantic_capabilities=semantic_capabilities,
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
        trade_distribution = self._trade_distribution(run.trades, semantic_capabilities=semantic_capabilities)
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
            "artifact_capabilities": semantic_capabilities,
        }

    def ingest_parameter_sweep_table(
        self,
        table_path: str | Path,
        *,
        parameter_names: list[str],
        run_id_column: str = "run_id",
    ) -> ParsedArtifactInput:
        frame = pd.read_csv(table_path)
        required_columns = {run_id_column, *parameter_names, "entry_time", "symbol", "side"}
        missing = sorted(required_columns - set(frame.columns))
        if missing:
            raise IngestionError(
                f"parameter_sweep table missing required columns: {missing}. "
                "Provide run_id, parameter columns, and canonical trade columns."
            )

        runs: list[ParameterSweepRunInput] = []
        flattened: list[NormalizedTradeRecord] = []
        for run_id, group in frame.groupby(run_id_column):
            params: dict[str, int | float | str | bool] = {}
            for name in parameter_names:
                unique_values = group[name].dropna().unique().tolist()
                if len(unique_values) != 1:
                    raise IngestionError(
                        f"Run '{run_id}' must have exactly one value for parameter '{name}'."
                    )
                params[name] = self._validate_parameter_value(unique_values[0], run_id=str(run_id), param_name=name)
            trades = self._records_from_trade_frame(group.drop(columns=parameter_names + [run_id_column]))
            runs.append(ParameterSweepRunInput(run_id=str(run_id), params=params, trades=trades))
            flattened.extend(trades)

        if len(runs) < 2:
            raise IngestionError("parameter_sweep table must contain at least two distinct run_id groups.")

        return ParsedArtifactInput(
            artifact_kind="parameter_sweep",
            richness="research_complete",
            trades=flattened,
            strategy_metadata={"strategy_name": Path(table_path).stem, "ingestion_contract": "parameter_sweep_table"},
            params={"parameter_names": parameter_names},
            parameter_sweep=ParameterSweepInput(parameter_names=parameter_names, runs=runs),
            parser_notes=["Parsed parameter sweep from combined table with run_id + parameter columns."],
        )

    def run_analysis_from_parsed_artifact(
        self,
        parsed_artifact: ParsedArtifactInput,
        *,
        config: AnalysisRunConfig | None = None,
    ) -> EngineAnalysisResult:
        config = config or AnalysisRunConfig()
        run = self._ingested_run_from_parsed_artifact(parsed_artifact)

        runtime_metadata: dict[str, Any] = {
            "seed": config.seed,
            "simulations": config.simulations,
            "ruin_drawdown_levels": list(config.ruin_drawdown_levels),
        }
        if config.account_size is not None:
            runtime_metadata["account_size"] = config.account_size
        if config.risk_per_trade_pct is not None:
            runtime_metadata["risk_per_trade_pct"] = config.risk_per_trade_pct
        prop_rules = self._normalize_prop_evaluation_rules(config.prop_evaluation_rules, account_size=config.account_size)
        runtime_metadata["prop_evaluation_rules"] = prop_rules

        benchmark_cfg = getattr(config, "benchmark", None)
        if isinstance(benchmark_cfg, dict):
            runtime_metadata["benchmark_config"] = benchmark_cfg
            run.metadata["benchmark_present"] = bool(benchmark_cfg.get("enabled", False))

        run.metadata.update(runtime_metadata)

        payload = self.build_dashboard_payload(
            run,
            seed=config.seed,
            simulations=config.simulations,
            ruin_drawdown_levels=config.ruin_drawdown_levels,
            account_size=config.account_size,
            risk_per_trade_pct=config.risk_per_trade_pct,
        )
        if parsed_artifact.parameter_sweep is not None:
            payload["parameter_stability"] = self._parameter_stability_from_parameter_sweep(
                parsed_artifact.parameter_sweep
            )
        payload["prop_evaluation_readiness"] = self._prop_evaluation_readiness(
            run=run,
            rules=prop_rules,
            config=config,
        )

        diagnostics = {
            "overview": payload["overview"],
            "distribution": payload["trade_distribution"],
            "monte_carlo": payload["monte_carlo"],
            "stability": payload["parameter_stability"],
            "execution": payload["execution_sensitivity"],
            "regimes": payload["regime_analysis"],
            "ruin": payload["risk_of_ruin"],
            "prop_evaluation_readiness": payload["prop_evaluation_readiness"],
            "report": payload["validation_report"],
        }

        artifact_capabilities = self._semantic_trade_capabilities(
            trades=run.trades,
            ohlcv_present=bool(parsed_artifact.ohlcv_present or parsed_artifact.ohlcv),
            benchmark_present=parsed_artifact.benchmark_present,
            params_present=parsed_artifact.params is not None,
            parameter_sweep_present=parsed_artifact.parameter_sweep is not None,
        )
        asset_class_capabilities = self._asset_class_capability_profile(parsed_artifact)
        artifact_capabilities.update(
            {
                "has_broker_export": bool(parsed_artifact.broker_export_present or parsed_artifact.broker_exports),
                "has_declared_claims": bool(parsed_artifact.declared_claims),
                "has_bundle_manifest": parsed_artifact.bundle_manifest is not None,
            }
        )
        capability_profile = AnalysisCapabilityProfile(
            diagnostics=self._diagnostic_capability_profile(
                parsed_artifact,
                config=config,
                artifact_capabilities=artifact_capabilities,
            ),
            artifact_capabilities=artifact_capabilities,
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
        evidence_facts = self._build_evidence_facts(
            parsed_artifact=parsed_artifact,
            diagnostics=diagnostics,
            capability_profile=capability_profile,
            asset_class_capabilities=asset_class_capabilities,
        )
        assumption_ledger = self._build_assumption_ledger(
            parsed_artifact=parsed_artifact,
            diagnostics=diagnostics,
            config=config,
        )
        claim_inventory = self._build_claim_inventory(
            parsed_artifact=parsed_artifact,
            diagnostics=diagnostics,
            capability_profile=capability_profile,
        )
        proof_report = self._build_proof_report_payload(
            parsed_artifact=parsed_artifact,
            diagnostics=diagnostics,
            evidence_facts=evidence_facts,
            assumption_ledger=assumption_ledger,
            claim_inventory=claim_inventory,
            asset_class_capabilities=asset_class_capabilities,
        )
        payload["strategy_truth_room"] = {
            "contract_version": STRATEGY_TRUTH_ROOM_CONTRACT_VERSION,
            "evidence_facts": evidence_facts,
            "assumption_ledger": assumption_ledger,
            "claim_inventory": claim_inventory,
            "proof_report": proof_report,
        }

        run_context = EngineRunContext(
            artifact_kind=parsed_artifact.artifact_kind,
            richness=parsed_artifact.richness,
            trade_count=len(parsed_artifact.trades),
            ohlcv_present=bool(parsed_artifact.ohlcv_present or parsed_artifact.ohlcv),
            benchmark_present=parsed_artifact.benchmark_present,
            has_assumptions=parsed_artifact.assumptions is not None,
            has_params=parsed_artifact.params is not None,
            has_parameter_sweep=parsed_artifact.parameter_sweep is not None,
            has_broker_export=bool(parsed_artifact.broker_export_present or parsed_artifact.broker_exports),
            has_declared_claims=bool(parsed_artifact.declared_claims),
            asset_class_capabilities=asset_class_capabilities,
        )
        return EngineAnalysisResult(
            envelope=EngineEnvelopeV1(
                engine_name="bt",
                engine_version=None,
                strategy_truth_room_contract_version=STRATEGY_TRUTH_ROOM_CONTRACT_VERSION,
                seam_name=ENGINE_SEAM_NAME,
                seam_version=ENGINE_SEAM_VERSION,
                adapter_version=ENGINE_ADAPTER_VERSION,
                parser_version=ENGINE_PARSER_VERSION,
                capability_profile_version=CAPABILITY_PROFILE_VERSION,
                diagnostic_contract_version=DIAGNOSTIC_CONTRACT_VERSION,
            ),
            run_context=run_context,
            capability_profile=capability_profile,
            warnings=warnings,
            diagnostics=diagnostics,
            raw_payload=payload,
            evidence_facts=evidence_facts,
            assumption_ledger=assumption_ledger,
            claim_inventory=claim_inventory,
            proof_report=proof_report,
        )

    def _ingested_run_from_parsed_artifact(self, parsed_artifact: ParsedArtifactInput) -> IngestedRun:
        trade_records = list(parsed_artifact.trades)
        if not trade_records and parsed_artifact.parameter_sweep is not None:
            for run in parsed_artifact.parameter_sweep.runs:
                trade_records.extend(run.trades)
        if not trade_records:
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
                "pnl_price": trade.gross_pnl,
                "mae_price": trade.mae,
                "mfe_price": trade.mfe,
                "stop_distance": trade.stop_distance,
                "risk_amount": trade.risk_amount,
                "slippage": trade.slippage,
                "r_multiple_net": trade.r_multiple_net,
                "r_multiple_gross": trade.r_multiple_gross,
                "strategy_name": trade.strategy_name,
                "timeframe": trade.timeframe,
                "market": trade.market,
                "exchange": trade.exchange,
            }
            for trade in trade_records
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
                "ohlcv_present": bool(parsed_artifact.ohlcv_present or parsed_artifact.ohlcv),
                "benchmark_present": parsed_artifact.benchmark_present,
                "params_present": parsed_artifact.params is not None,
                "assumptions_present": parsed_artifact.assumptions is not None,
                "broker_export_present": bool(parsed_artifact.broker_export_present or parsed_artifact.broker_exports),
                "declared_claims_present": bool(parsed_artifact.declared_claims),
                "source_file_count": len(parsed_artifact.source_files),
                "equity_curve_provenance": equity_curve_provenance,
            }
        )
        if parsed_artifact.bundle_manifest is not None:
            metadata["bundle_manifest"] = parsed_artifact.bundle_manifest
        if parsed_artifact.ohlcv:
            metadata["_ohlcv_context"] = self._normalize_ohlcv(parsed_artifact.ohlcv)

        return IngestedRun(
            source="parsed_artifact",
            trades=normalized,
            equity=equity,
            performance=performance,
            metadata=metadata,
        )

    def _semantic_trade_capabilities(
        self,
        *,
        trades: pd.DataFrame,
        ohlcv_present: bool,
        benchmark_present: bool,
        params_present: bool,
        parameter_sweep_present: bool,
    ) -> dict[str, bool]:
        def _has_non_null(column: str) -> bool:
            return bool(column in trades.columns and trades[column].notna().any())

        has_trade_timestamps = _has_non_null("entry_ts")
        has_exit_timestamps = _has_non_null("exit_ts")
        has_entry_exit_prices = _has_non_null("entry_price") and _has_non_null("exit_price")
        has_quantity = _has_non_null("quantity")
        has_net_pnl = _has_non_null("pnl_net")
        has_gross_pnl = _has_non_null("pnl_price")
        has_fee_fields = _has_non_null("fees_paid")
        has_slippage_fields = _has_non_null("slippage")
        has_cost_fields = has_fee_fields or has_slippage_fields or _has_non_null("spread")
        has_excursion_fields = _has_non_null("mae_price") and _has_non_null("mfe_price")
        has_risk_fields = _has_non_null("risk_amount")
        has_stop_distance_fields = _has_non_null("stop_distance")
        has_r_multiple_fields = _has_non_null("r_multiple_net") or _has_non_null("r_multiple_gross")
        has_equity_series = has_net_pnl
        has_market_context = bool(ohlcv_present)
        has_benchmark_context = bool(benchmark_present)
        has_parameter_grid = bool(params_present or parameter_sweep_present)

        return {
            "has_trade_timestamps": has_trade_timestamps,
            "has_exit_timestamps": has_exit_timestamps,
            "has_entry_exit_prices": has_entry_exit_prices,
            "has_quantity": has_quantity,
            "has_net_pnl": has_net_pnl,
            "has_gross_pnl": has_gross_pnl,
            "has_cost_fields": has_cost_fields,
            "has_fee_fields": has_fee_fields,
            "has_slippage_fields": has_slippage_fields,
            "has_excursion_fields": has_excursion_fields,
            "has_risk_fields": has_risk_fields,
            "has_stop_distance_fields": has_stop_distance_fields,
            "has_r_multiple_fields": has_r_multiple_fields,
            "has_equity_series": has_equity_series,
            "has_market_context": has_market_context,
            "has_benchmark_context": has_benchmark_context,
            "has_parameter_grid": has_parameter_grid,
            "can_build_equity_curve": has_equity_series and has_trade_timestamps,
            "can_build_duration_distribution": has_trade_timestamps and has_exit_timestamps,
            "can_build_histogram_from_returns": has_net_pnl,
            "can_build_histogram_from_r_multiples": has_r_multiple_fields,
            "can_build_mae_mfe_scatter": has_excursion_fields,
            "can_build_cost_drag_summary": has_net_pnl and (has_cost_fields or has_gross_pnl),
            "can_build_execution_sensitivity_baseline": has_net_pnl,
            "can_build_monte_carlo_paths": has_net_pnl,
            "can_build_ruin_model": has_net_pnl and has_risk_fields,
            "can_build_regime_analysis": has_market_context and has_net_pnl,
            "can_build_parameter_stability": has_parameter_grid,
            "can_build_trade_distribution": has_net_pnl,
        }

    def _asset_class_capability_profile(self, parsed_artifact: ParsedArtifactInput) -> dict[str, Any]:
        symbols = {
            str(trade.symbol).upper()
            for trade in parsed_artifact.trades
            if getattr(trade, "symbol", None)
        }
        markets = {
            str(trade.market).lower()
            for trade in parsed_artifact.trades
            if getattr(trade, "market", None)
        }
        exchanges = {
            str(trade.exchange).lower()
            for trade in parsed_artifact.trades
            if getattr(trade, "exchange", None)
        }
        metadata = parsed_artifact.strategy_metadata or {}
        declared_asset_class = str(
            metadata.get("asset_class")
            or metadata.get("market")
            or metadata.get("assetClass")
            or ""
        ).lower()

        joined = " ".join([declared_asset_class, *symbols, *markets, *exchanges])
        if any(token in joined for token in ("crypto", "btc", "eth", "usdt", "binance", "bybit", "coinbase")):
            detected = "crypto"
        elif any(token in joined for token in ("forex", "fx", "eurusd", "gbpusd", "usdjpy")):
            detected = "fx"
        elif any(token in joined for token in ("futures", "cme", "contract", "perp")):
            detected = "futures_or_cfd"
        elif any(token in joined for token in ("index", "spy", "qqq", "nasdaq", "s&p")):
            detected = "index_or_etf"
        elif symbols:
            detected = "listed_equity_or_unknown_symbol"
        else:
            detected = "unknown"

        has_contract_terms = bool(
            metadata.get("contract_multiplier")
            or metadata.get("tick_value")
            or metadata.get("margin_model")
        )
        broker_grade = bool(parsed_artifact.broker_export_present or parsed_artifact.broker_exports)
        venue_supported = bool(exchanges or metadata.get("exchange") or metadata.get("broker"))

        if detected == "futures_or_cfd" and not has_contract_terms:
            confidence = "limited"
            limitation = "Futures/CFD validation is limited without contract multiplier, tick value, margin, and session details."
        elif broker_grade:
            confidence = "supported"
            limitation = None
        elif venue_supported:
            confidence = "limited"
            limitation = "Asset-class recognition is supported, but execution realism remains limited without broker/fill exports."
        else:
            confidence = "limited"
            limitation = "Asset class inferred from symbols; broker, venue, and contract details are missing."

        return {
            "detected_asset_class": detected,
            "confidence": confidence,
            "broker_grade_execution_available": broker_grade,
            "venue_context_available": venue_supported,
            "contract_terms_available": has_contract_terms,
            "limitations": [limitation] if limitation else [],
        }

    def _build_evidence_facts(
        self,
        *,
        parsed_artifact: ParsedArtifactInput,
        diagnostics: dict[str, dict[str, Any]],
        capability_profile: AnalysisCapabilityProfile,
        asset_class_capabilities: dict[str, Any],
    ) -> list[dict[str, Any]]:
        facts: list[dict[str, Any]] = [
            {
                "fact_id": "artifact.trade_count",
                "source": "parsed_artifact.trades",
                "statement": f"{len(parsed_artifact.trades)} normalized trades were supplied.",
                "value": len(parsed_artifact.trades),
                "diagnostics": ["overview", "distribution", "monte_carlo", "execution", "ruin", "report"],
                "confidence": "high" if parsed_artifact.trades else "low",
            },
            {
                "fact_id": "artifact.asset_class",
                "source": "strategy_metadata",
                "statement": f"Asset class classified as {asset_class_capabilities.get('detected_asset_class', 'unknown')}.",
                "value": asset_class_capabilities.get("detected_asset_class"),
                "diagnostics": ["overview", "execution", "report"],
                "confidence": asset_class_capabilities.get("confidence", "limited"),
            },
            {
                "fact_id": "artifact.broker_export",
                "source": "broker_exports",
                "statement": "Broker/fill export was supplied." if parsed_artifact.broker_export_present or parsed_artifact.broker_exports else "Broker/fill export was not supplied.",
                "value": bool(parsed_artifact.broker_export_present or parsed_artifact.broker_exports),
                "diagnostics": ["execution", "report"],
                "confidence": "high" if parsed_artifact.broker_export_present or parsed_artifact.broker_exports else "limited",
            },
            {
                "fact_id": "artifact.market_context",
                "source": "ohlcv",
                "statement": "OHLCV market context was supplied." if parsed_artifact.ohlcv_present or parsed_artifact.ohlcv else "OHLCV market context was not supplied.",
                "value": bool(parsed_artifact.ohlcv_present or parsed_artifact.ohlcv),
                "diagnostics": ["regimes", "monte_carlo", "report"],
                "confidence": "high" if parsed_artifact.ohlcv_present or parsed_artifact.ohlcv else "limited",
            },
        ]
        for diagnostic, capability in capability_profile.diagnostics.items():
            facts.append(
                {
                    "fact_id": f"diagnostic.{diagnostic}.status",
                    "source": "capability_profile",
                    "statement": f"{diagnostic} is {capability.status}: {capability.reason}",
                    "value": capability.status,
                    "diagnostics": [diagnostic],
                    "confidence": "high",
                }
            )
        return facts

    def _build_assumption_ledger(
        self,
        *,
        parsed_artifact: ParsedArtifactInput,
        diagnostics: dict[str, dict[str, Any]],
        config: AnalysisRunConfig,
    ) -> list[dict[str, Any]]:
        ledger: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        def add(
            *,
            source: str,
            diagnostic: str,
            statement: str,
            materiality: str,
            confidence: str,
            rescue_evidence: str,
            affected_metrics: list[str] | None = None,
        ) -> None:
            key = (diagnostic, statement)
            if key in seen:
                return
            seen.add(key)
            ledger.append(
                {
                    "assumption_id": f"A{len(ledger) + 1:03d}",
                    "source": source,
                    "diagnostic": diagnostic,
                    "statement": statement,
                    "materiality": materiality,
                    "confidence": confidence,
                    "testability": "testable_with_additional_artifact",
                    "affected_metrics": affected_metrics or [],
                    "verdict_impact": "Can weaken or strengthen the final validation verdict.",
                    "rescue_evidence": rescue_evidence,
                    "share_safe": True,
                }
            )

        if parsed_artifact.assumptions:
            for key, value in parsed_artifact.assumptions.items():
                add(
                    source="user_declared",
                    diagnostic="report",
                    statement=f"{key}: {value}",
                    materiality="medium",
                    confidence="user_declared",
                    rescue_evidence="Attach source configuration or broker/export evidence supporting this assumption.",
                )
        for diagnostic, payload in diagnostics.items():
            for statement in payload.get("assumptions", []) or []:
                add(
                    source="engine_emitted",
                    diagnostic=diagnostic,
                    statement=str(statement),
                    materiality="high" if diagnostic in {"execution", "ruin", "monte_carlo"} else "medium",
                    confidence="engine_emitted",
                    rescue_evidence=f"Provide richer artifacts for {diagnostic} if this assumption is too broad.",
                )
        if config.account_size is None:
            add(
                source="missing_input",
                diagnostic="ruin",
                statement="account_size was not explicitly provided.",
                materiality="critical",
                confidence="high",
                affected_metrics=["probability_of_ruin", "survival_probability"],
                rescue_evidence="Provide account_size in runtime configuration or bundle assumptions.",
            )
        if config.risk_per_trade_pct is None:
            add(
                source="missing_input",
                diagnostic="ruin",
                statement="risk_per_trade_pct was not explicitly provided.",
                materiality="critical",
                confidence="high",
                affected_metrics=["probability_of_ruin", "max_tolerable_risk_per_trade"],
                rescue_evidence="Provide risk_per_trade_pct in runtime configuration or bundle assumptions.",
            )
        add(
            source="runtime_config",
            diagnostic="prop_evaluation_readiness",
            statement=(
                "Prop evaluation readiness used fallback preview rules."
                if not (config.prop_evaluation_rules or {}).get("source")
                else f"Prop evaluation readiness used {config.prop_evaluation_rules.get('source')} rules."
            ),
            materiality="high",
            confidence="runtime_config",
            affected_metrics=["prop_evaluation_verdict", "first_breach", "target_before_breach_probability"],
            rescue_evidence="Enter the exact prop firm evaluation rules and rerun the readiness analysis.",
        )
        if not (parsed_artifact.broker_export_present or parsed_artifact.broker_exports):
            add(
                source="missing_input",
                diagnostic="execution",
                statement="Execution realism is limited because broker/fill exports were not supplied.",
                materiality="critical",
                confidence="high",
                affected_metrics=["edge_decay_pct", "cost_kill_threshold_bps"],
                rescue_evidence="Attach broker/fill exports with fees, fills, timestamps, venue, and order ids.",
            )
        if not (parsed_artifact.ohlcv_present or parsed_artifact.ohlcv):
            add(
                source="missing_input",
                diagnostic="regimes",
                statement="Regime conclusions are unavailable or limited without OHLCV market context.",
                materiality="high",
                confidence="high",
                affected_metrics=["regime_dependence", "adverse_regime_decay"],
                rescue_evidence="Attach OHLCV context aligned to trade timestamps.",
            )
        return ledger

    def _build_claim_inventory(
        self,
        *,
        parsed_artifact: ParsedArtifactInput,
        diagnostics: dict[str, dict[str, Any]],
        capability_profile: AnalysisCapabilityProfile,
    ) -> list[dict[str, Any]]:
        declared = parsed_artifact.declared_claims or []
        if not declared:
            declared = [
                {
                    "claim_id": "auto_positive_edge",
                    "claim": "The strategy has positive edge after costs.",
                    "source": "engine_default",
                    "priority": "high",
                },
                {
                    "claim_id": "auto_execution_realism",
                    "claim": "The backtest execution assumptions are realistic enough for decisioning.",
                    "source": "engine_default",
                    "priority": "critical",
                },
                {
                    "claim_id": "auto_regime_robustness",
                    "claim": "The edge persists outside the favorable regime.",
                    "source": "engine_default",
                    "priority": "high",
                },
            ]

        execution_status = capability_profile.diagnostics["execution"].status
        regimes_status = capability_profile.diagnostics["regimes"].status
        distribution = diagnostics.get("distribution", {})
        execution = diagnostics.get("execution", {})
        expectancy = distribution.get("summary_metrics", {}).get("expectancy")
        edge_decay = execution.get("summary_metrics", {}).get("edge_decay_pct")
        inventory: list[dict[str, Any]] = []
        for index, raw_claim in enumerate(declared):
            claim_text = str(raw_claim.get("claim") or raw_claim.get("text") or raw_claim)
            lowered = claim_text.lower()
            status = "unsupported"
            supporting = []
            contradicting = []
            missing = []
            if "edge" in lowered or "expectancy" in lowered or "profit" in lowered:
                supporting.append("distribution")
                if isinstance(expectancy, (int, float)) and expectancy > 0:
                    status = "partially_supported" if execution_status != "supported" else "supported"
                else:
                    status = "contradicted"
                    contradicting.append("distribution")
                if execution_status != "supported":
                    missing.append("broker/fill export or execution cost calibration")
            elif "execution" in lowered or "fill" in lowered or "slippage" in lowered:
                supporting.append("execution")
                status = "supported" if execution_status == "supported" and edge_decay is not None else "partially_supported"
                if not (parsed_artifact.broker_export_present or parsed_artifact.broker_exports):
                    status = "unsupported"
                    missing.append("broker/fill export")
            elif "regime" in lowered:
                supporting.append("regimes")
                status = "supported" if regimes_status == "supported" else "unsupported"
                if regimes_status != "supported":
                    missing.append("OHLCV or regime-labeled context")
            else:
                missing.append("explicit mapping from claim to diagnostic evidence")

            inventory.append(
                {
                    "claim_id": str(raw_claim.get("claim_id") or f"claim_{index + 1}"),
                    "claim": claim_text,
                    "source": str(raw_claim.get("source") or "declared_claims"),
                    "priority": str(raw_claim.get("priority") or "medium"),
                    "support_status": status,
                    "supporting_diagnostics": supporting,
                    "contradicting_diagnostics": contradicting,
                    "missing_evidence": missing,
                    "report_wording": f"Claim is {status.replace('_', ' ')} under the supplied artifact bundle.",
                }
            )
        return inventory

    def _build_proof_report_payload(
        self,
        *,
        parsed_artifact: ParsedArtifactInput,
        diagnostics: dict[str, dict[str, Any]],
        evidence_facts: list[dict[str, Any]],
        assumption_ledger: list[dict[str, Any]],
        claim_inventory: list[dict[str, Any]],
        asset_class_capabilities: dict[str, Any],
    ) -> dict[str, Any]:
        unavailable = [
            name for name, payload in diagnostics.items()
            if payload.get("status") in {"unavailable", "skipped"}
        ]
        limited = [
            name for name, payload in diagnostics.items()
            if payload.get("status") == "limited"
        ]
        unsupported_claims = [
            claim for claim in claim_inventory
            if claim.get("support_status") in {"unsupported", "contradicted"}
        ]
        available = [name for name, payload in diagnostics.items() if payload.get("status") == "available"]
        diagnostic_confidence = {
            name: {
                "status": payload.get("status", "unavailable"),
                "confidence": (
                    "high" if payload.get("status") == "available"
                    else "medium" if payload.get("status") == "limited"
                    else "low"
                ),
                "limitations": payload.get("limitations", [])[:5],
                "reason_codes": payload.get("reason_codes", []),
                "next_evidence": payload.get("next_evidence", []),
            }
            for name, payload in diagnostics.items()
        }
        falsification_results = {
            "execution": diagnostics.get("execution", {}).get("sensitivity_classification"),
            "rare_trade_dependence": diagnostics.get("distribution", {}).get("summary_metrics", {}).get("rare_trade_dependence"),
            "monte_carlo": diagnostics.get("monte_carlo", {}).get("summary_metrics", {}),
            "ruin": diagnostics.get("ruin", {}).get("summary_metrics", {}),
            "prop_evaluation_readiness": {
                "verdict": diagnostics.get("prop_evaluation_readiness", {}).get("verdict"),
                "summary_metrics": diagnostics.get("prop_evaluation_readiness", {}).get("summary_metrics", {}),
                "rule_snapshot": diagnostics.get("prop_evaluation_readiness", {}).get("rule_snapshot", {}),
                "first_breach": diagnostics.get("prop_evaluation_readiness", {}).get("first_breach"),
                "limitations": diagnostics.get("prop_evaluation_readiness", {}).get("limitations", []),
            },
            "regime": diagnostics.get("regimes", {}).get("classification"),
            "stability": diagnostics.get("stability", {}).get("summary_metrics", {}),
        }
        taxonomy_verdict = self._strategy_truth_room_verdict(
            diagnostics=diagnostics,
            unsupported_claims=unsupported_claims,
            unavailable=unavailable,
            limited=limited,
        )
        next_evidence = sorted({
            *(str(item) for claim in unsupported_claims for item in claim.get("missing_evidence", [])),
            *(str(item.get("rescue_evidence")) for item in assumption_ledger if item.get("rescue_evidence")),
            *(f"Add evidence required by {name} diagnostic." for name in unavailable),
        })
        return {
            "contract_version": STRATEGY_TRUTH_ROOM_CONTRACT_VERSION,
            "executive_verdict": {
                "taxonomy": taxonomy_verdict,
                "allowed_verdicts": list(STRATEGY_TRUTH_ROOM_VERDICTS),
                "summary": "Verdict is a validation classification, not investment advice.",
            },
            "artifact_identity": {
                "artifact_kind": parsed_artifact.artifact_kind,
                "richness": parsed_artifact.richness,
                "source_files": parsed_artifact.source_files,
                "bundle_manifest": parsed_artifact.bundle_manifest,
            },
            "evidence_coverage": {
                "available": available,
                "limited": limited,
                "unavailable": unavailable,
            },
            "diagnostic_confidence": diagnostic_confidence,
            "falsification_results": falsification_results,
            "asset_class_capabilities": asset_class_capabilities,
            "assumptions": assumption_ledger,
            "critical_assumptions": [
                item for item in assumption_ledger
                if item.get("materiality") in {"critical", "high"}
            ],
            "unsupported_claims": unsupported_claims,
            "limitations": [
                *(str(limitation) for payload in diagnostics.values() for limitation in payload.get("limitations", [])[:3]),
                *(f"{name} diagnostic was not fully available." for name in unavailable),
            ],
            "what_this_result_does_not_prove": [
                *(f"Does not fully support claim: {claim.get('claim')}" for claim in unsupported_claims),
                *(f"{name} diagnostic was not fully available." for name in unavailable),
            ],
            "next_evidence": next_evidence,
            "research_desk_packet_hooks": {
                "recommended": bool(unsupported_claims or unavailable or limited),
                "suggested_scopes": sorted({
                    *("execution_audit" for claim in unsupported_claims if "execution" in claim.get("missing_evidence", [])),
                    *("claim_validation" for _ in unsupported_claims),
                    *("regime_context_review" for name in unavailable if name == "regimes"),
                    *("parameter_stability_review" for name in unavailable if name == "stability"),
                }),
            },
            "evidence_facts": evidence_facts,
        }

    def _strategy_truth_room_verdict(
        self,
        *,
        diagnostics: dict[str, dict[str, Any]],
        unsupported_claims: list[dict[str, Any]],
        unavailable: list[str],
        limited: list[str],
    ) -> str:
        execution = diagnostics.get("execution", {})
        regimes = diagnostics.get("regimes", {})
        stability = diagnostics.get("stability", {})
        if len(unavailable) >= 4 or "overview" in unavailable:
            return "data_insufficient"
        if execution.get("sensitivity_classification") in {"cost_killed", "negative"}:
            return "untradeable_after_costs"
        if execution.get("execution_fantasy_triggers"):
            return "execution_fantasy"
        if regimes.get("classification") in {"regime_dependent", "fragile"}:
            return "regime_dependent"
        if stability.get("classification") in {"fragile", "cliff"} or stability.get("status") in {"limited", "unavailable"}:
            return "likely_overfit"
        if unsupported_claims or limited:
            return "promising_but_under_supported"
        return "structurally_credible"

    def _diagnostic_capability_profile(
        self,
        parsed_artifact: ParsedArtifactInput,
        *,
        config: AnalysisRunConfig,
        artifact_capabilities: dict[str, bool],
    ) -> dict[str, DiagnosticCapability]:
        trade_count = len(parsed_artifact.trades)
        has_trades = trade_count > 0
        has_params = bool(artifact_capabilities.get("has_parameter_grid"))
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
        has_ohlcv_context = bool(artifact_capabilities.get("has_market_context"))
        if has_trades and not has_ohlcv_context:
            regimes = DiagnosticCapability(
                status="unavailable",
                reason="Regime analysis requires OHLCV or equivalent market context.",
                required_inputs=["trades", "ohlcv"],
                optional_enrichments=["benchmark", "params"],
            )

        report_status = "supported" if has_trades else "unavailable"
        report_reason = (
            "Report synthesized from available diagnostics."
            if has_trades
            else "Cannot assemble report without diagnostics from trade data."
        )

        if not has_trades:
            ruin_capability = DiagnosticCapability(
                status="unavailable",
                reason="No trades supplied in parsed artifact.",
                required_inputs=["trades", "account_size", "risk_per_trade_pct"],
                optional_enrichments=["assumptions", "stop_policy", "compounding_model"],
            )
        elif config.account_size is None or config.risk_per_trade_pct is None:
            missing = []
            if config.account_size is None:
                missing.append("account_size")
            if config.risk_per_trade_pct is None:
                missing.append("risk_per_trade_pct")
            ruin_capability = DiagnosticCapability(
                status="limited",
                reason=(
                    "Full risk-of-ruin model requires explicit "
                    + ", ".join(missing)
                    + " assumptions."
                ),
                required_inputs=["trades", "account_size", "risk_per_trade_pct"],
                optional_enrichments=["assumptions", "stop_policy", "compounding_model", "monte_carlo"],
            )
        elif trade_count < 30:
            ruin_capability = DiagnosticCapability(
                status="limited",
                reason="Ruin estimates are computed from fewer than 30 trades and have high uncertainty.",
                required_inputs=["trades", "account_size", "risk_per_trade_pct"],
                optional_enrichments=["assumptions", "stop_policy", "compounding_model", "monte_carlo"],
            )
        elif config.simulations <= 0:
            ruin_capability = DiagnosticCapability(
                status="unavailable",
                reason="Ruin model requires Monte Carlo simulations > 0 for survivability estimates.",
                required_inputs=["trades", "account_size", "risk_per_trade_pct"],
                optional_enrichments=["assumptions", "stop_policy", "compounding_model", "monte_carlo"],
            )
        else:
            ruin_capability = DiagnosticCapability(
                status="supported",
                reason="Risk-of-ruin model active with explicit capital and risk-per-trade assumptions.",
                required_inputs=["trades", "account_size", "risk_per_trade_pct"],
                optional_enrichments=["assumptions", "stop_policy", "compounding_model", "monte_carlo"],
            )

        return {
            "overview": status_for_trade_based("overview"),
            "distribution": (
                status_for_trade_based("distribution")
                if artifact_capabilities.get("can_build_trade_distribution")
                else DiagnosticCapability(
                    status="unavailable",
                    reason="Trade distribution requires normalized trade outcomes.",
                    required_inputs=["trades", "pnl_net"],
                    optional_enrichments=["duration", "mae_mfe", "r_multiple"],
                )
            ),
            "monte_carlo": (
                status_for_trade_based("monte_carlo")
                if artifact_capabilities.get("can_build_monte_carlo_paths")
                else DiagnosticCapability(
                    status="unavailable",
                    reason="Monte Carlo requires normalized per-trade return outcomes.",
                    required_inputs=["trades", "pnl_net"],
                    optional_enrichments=["equity_curve", "account_size"],
                )
            ),
            "stability": stability,
            "execution": (
                DiagnosticCapability(
                    status="supported" if has_trades else "unavailable",
                    reason=(
                        "Execution sensitivity computed from fees/slippage/spread when present; broker-grade realism requires fill or broker export context."
                        if has_trades
                        else "No trades supplied in parsed artifact."
                    ),
                    required_inputs=["trades"],
                    optional_enrichments=["assumptions", "broker_export", "fees", "slippage", "spread", "params"],
                )
            ),
            "regimes": regimes,
            "ruin": ruin_capability,
            "prop_evaluation_readiness": DiagnosticCapability(
                status="limited" if has_trades else "unavailable",
                reason=(
                    "Prop evaluation readiness can run from trade/equity path, but daily-loss precision depends on intraday equity, account sizing, and exact firm rules."
                    if has_trades
                    else "No trades supplied in parsed artifact."
                ),
                required_inputs=["trades", "prop_evaluation_rules"],
                optional_enrichments=["equity_curve", "broker_export", "intraday_equity", "account_size", "risk_per_trade_pct"],
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
                "available": False,
                "limited": False,
                "reason_unavailable": reason,
                "reason_codes": [self._diagnostic_reason_code(name, reason=reason, status="skipped")],
                "next_evidence": self._next_evidence_for_diagnostic(name, reason=reason),
                "summary_metrics": {},
                "figures": [],
                "interpretation": None,
                "warnings": [],
                "assumptions": [],
                "limitations": [reason] if reason else [],
                "recommendations": [],
                "metadata": {
                    "skip_reason": reason,
                    "compatibility": {
                        "reason": reason,
                    },
                },
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
            reason = str(payload.get("reason_unavailable") or capability.reason or "diagnostic skipped")
            return {
                "status": "skipped",
                "available": False,
                "limited": False,
                "reason_unavailable": reason,
                "reason_codes": payload.get("reason_codes") or [self._diagnostic_reason_code(name, reason=reason, status="skipped")],
                "next_evidence": payload.get("next_evidence") or self._next_evidence_for_diagnostic(name, reason=reason),
                "summary_metrics": payload.get("summary_metrics", {}),
                "figures": payload.get("figures", []),
                "interpretation": self._normalize_interpretation(payload.get("interpretation"), name=name),
                "warnings": payload.get("warnings", []),
                "assumptions": payload.get("assumptions", []),
                "limitations": payload.get("limitations", []),
                "recommendations": payload.get("recommendations", []),
                "metadata": payload.get("metadata", {}),
                "payload": payload,
            }

        available = capability.status != "unavailable"
        limited = capability.status == "limited"
        reason_unavailable = None if available else capability.reason
        payload_limitations = payload.get("limitations", [])
        limitations: list[str] = []
        if isinstance(payload_limitations, list):
            limitations.extend(str(item) for item in payload_limitations)
        if capability.reason:
            limitations.append(capability.reason)
        limitations = list(dict.fromkeys(limitations))
        assumptions = payload.get("assumptions", [])
        if name == "report":
            assumptions = self._unwrap_report_assumptions(payload)

        status = "unavailable" if not available else ("limited" if limited else "available")
        decorated: dict[str, Any] = {
            "status": status,
            "available": available,
            "limited": limited,
            "reason_unavailable": reason_unavailable,
            "reason_codes": [self._diagnostic_reason_code(name, reason=capability.reason, status=status)],
            "next_evidence": self._next_evidence_for_diagnostic(name, reason=capability.reason),
            "limitations": limitations,
            "summary_metrics": payload.get("summary_metrics", {}),
            "figures": payload.get("figures", []),
            "interpretation": self._normalize_interpretation(payload.get("interpretation"), name=name),
            "warnings": payload.get("warnings", []),
            "assumptions": assumptions,
            "recommendations": payload.get("recommendations", []),
            "metadata": payload.get("metadata", {}),
            "payload": payload,
        }
        if name == "report":
            decorated["report"] = payload.get("report", {})
        if "status" in payload and payload.get("status") != status:
            decorated["source_status"] = payload.get("status")
        reserved = set(decorated.keys())
        reserved.update({"status", "available", "limited", "reason_unavailable"})
        for key, value in payload.items():
            if key in reserved:
                continue
            decorated[key] = value
        return decorated

    def _diagnostic_reason_code(self, diagnostic: str, *, reason: str, status: str) -> str:
        lowered = str(reason or "").lower()
        if status == "available":
            return f"{diagnostic}.available"
        if "broker" in lowered or "fill" in lowered or "execution" in lowered or "slippage" in lowered:
            suffix = "execution_context_missing"
        elif "ohlcv" in lowered or "regime" in lowered or "market context" in lowered:
            suffix = "market_context_missing"
        elif "parameter" in lowered or "sweep" in lowered:
            suffix = "parameter_sweep_missing"
        elif "account" in lowered or "risk" in lowered or "sizing" in lowered:
            suffix = "risk_config_missing"
        elif "eligibility" in lowered or "skipped" in lowered:
            suffix = "artifact_policy_skipped"
        else:
            suffix = "evidence_limited" if status == "limited" else "evidence_unavailable"
        return f"{diagnostic}.{suffix}"

    def _next_evidence_for_diagnostic(self, diagnostic: str, *, reason: str) -> list[str]:
        lowered = str(reason or "").lower()
        if diagnostic == "execution" or "broker" in lowered or "fill" in lowered:
            return ["Attach broker/fill exports with timestamps, venue, fees, order ids, and realized fill prices."]
        if diagnostic == "regimes" or "ohlcv" in lowered or "market context" in lowered:
            return ["Attach OHLCV or regime-labeled context aligned to trade timestamps."]
        if diagnostic == "stability" or "parameter" in lowered or "sweep" in lowered:
            return ["Attach a parameter sweep bundle with objective, parameter names, and run-level results."]
        if diagnostic == "ruin" or "account" in lowered or "risk" in lowered:
            return ["Provide account size, risk-per-trade, sizing assumptions, and capital constraints."]
        if diagnostic == "report":
            return ["Generate a report snapshot after diagnostics complete and evidence limits are reviewed."]
        return ["Attach richer source artifacts or declared assumptions for this diagnostic."]

    def _normalize_interpretation(self, interpretation: Any, *, name: str) -> dict[str, Any] | None:
        if interpretation is None:
            return None
        if isinstance(interpretation, dict):
            summary = interpretation.get("summary")
            positives = interpretation.get("positives", [])
            cautions = interpretation.get("cautions", [])
            normalized: dict[str, Any] = {
                "summary": str(summary) if summary is not None else "",
                "positives": [str(item) for item in positives] if isinstance(positives, list) else [],
                "cautions": [str(item) for item in cautions] if isinstance(cautions, list) else [],
            }
            for key, value in interpretation.items():
                if key not in normalized:
                    normalized[key] = value
            return normalized
        if isinstance(interpretation, list):
            lines = [str(item) for item in interpretation if str(item).strip()]
            return {
                "summary": lines[0] if lines else "",
                "positives": [],
                "cautions": lines[1:] if len(lines) > 1 else [],
            }
        if isinstance(interpretation, str):
            message = interpretation.strip()
            return {
                "summary": message,
                "positives": [],
                "cautions": [],
            }
        return {
            "summary": f"{name} interpretation emitted unsupported type {type(interpretation).__name__}.",
            "positives": [],
            "cautions": [],
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

    def _parse_parameter_sweep_run_spec(
        self,
        *,
        root: Path,
        run_spec: Any,
        parameter_names: list[str],
        metric: str,
    ) -> ParameterSweepRunInput:
        if not isinstance(run_spec, dict):
            raise IngestionError("Each parameter_sweep run entry must be an object.")
        run_id = str(run_spec.get("run_id", "")).strip()
        if not run_id:
            raise IngestionError("Each parameter_sweep run requires non-empty 'run_id'.")

        raw_params = run_spec.get("params")
        if not isinstance(raw_params, dict):
            raise IngestionError(f"Run '{run_id}' must include a 'params' object.")
        if set(raw_params.keys()) != set(parameter_names):
            raise IngestionError(
                f"Run '{run_id}' parameter keys must exactly match manifest parameter_names."
            )

        parsed_params = {
            name: self._validate_parameter_value(raw_params[name], run_id=run_id, param_name=name)
            for name in parameter_names
        }
        summary = run_spec.get("summary") if isinstance(run_spec.get("summary"), dict) else None

        trades_file = run_spec.get("trades_file")
        trade_payload = run_spec.get("trades")
        if trades_file is None and trade_payload is None and (summary is None or metric not in summary):
            raise IngestionError(
                f"Run '{run_id}' must include trade data (trades_file/trades) or summary metric '{metric}'."
            )

        trades: list[NormalizedTradeRecord] = []
        if trades_file is not None:
            trades_path = (root / str(trades_file)).resolve()
            try:
                trades_path.relative_to(root.resolve())
            except ValueError as exc:
                raise IngestionError(f"Run '{run_id}' trades_file must stay within bundle directory.") from exc
            if not trades_path.exists():
                raise IngestionError(f"Run '{run_id}' trades_file not found: {trades_path}")
            trades = self._records_from_trade_frame(pd.read_csv(trades_path))
        elif isinstance(trade_payload, list) and trade_payload:
            trades = self._records_from_trade_frame(pd.DataFrame(trade_payload))

        return ParameterSweepRunInput(
            run_id=run_id,
            params=parsed_params,
            trades=trades,
            summary=summary,
            metadata=run_spec.get("metadata") if isinstance(run_spec.get("metadata"), dict) else None,
        )

    def _validate_parameter_value(
        self,
        value: Any,
        *,
        run_id: str,
        param_name: str,
    ) -> int | float | str | bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            parsed = float(value)
            if not np.isfinite(parsed):
                raise IngestionError(
                    f"Run '{run_id}' parameter '{param_name}' must be finite."
                )
            return int(value) if isinstance(value, int) else parsed
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise IngestionError(f"Run '{run_id}' parameter '{param_name}' cannot be empty.")
            return stripped
        raise IngestionError(
            f"Run '{run_id}' parameter '{param_name}' must be one of {_PARAM_VALUE_TYPES}."
        )

    def _records_from_trade_frame(self, trades: pd.DataFrame) -> list[NormalizedTradeRecord]:
        normalized = self._normalize_trades(trades)

        def _safe_float(value: Any) -> float | None:
            if pd.isna(value):
                return None
            parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            return float(parsed) if pd.notna(parsed) else None

        records: list[NormalizedTradeRecord] = []
        for row in normalized.to_dict(orient="records"):
            records.append(
                NormalizedTradeRecord(
                    symbol=str(row["symbol"]),
                    side=str(row["side"]),
                    entry_time=pd.Timestamp(row["entry_ts"]).isoformat().replace("+00:00", "Z"),
                    exit_time=(
                        pd.Timestamp(row["exit_ts"]).isoformat().replace("+00:00", "Z")
                        if pd.notna(row.get("exit_ts"))
                        else None
                    ),
                    entry_price=_safe_float(row.get("entry_price")),
                    exit_price=_safe_float(row.get("exit_price")),
                    quantity=_safe_float(row.get("quantity")),
                    fees=_safe_float(row.get("fees_paid")),
                    pnl=_safe_float(row.get("pnl_net")),
                    gross_pnl=_safe_float(row.get("pnl_price")),
                    slippage=_safe_float(row.get("slippage")),
                    risk_amount=_safe_float(row.get("risk_amount")),
                    stop_distance=_safe_float(row.get("stop_distance")),
                    r_multiple_net=_safe_float(row.get("r_multiple_net")),
                    r_multiple_gross=_safe_float(row.get("r_multiple_gross")),
                    mae=_safe_float(row.get("mae_price")),
                    mfe=_safe_float(row.get("mfe_price")),
                    strategy_name=str(row["strategy_name"]) if pd.notna(row.get("strategy_name")) else None,
                    timeframe=str(row["timeframe"]) if pd.notna(row.get("timeframe")) else None,
                    market=str(row["market"]) if pd.notna(row.get("market")) else None,
                    exchange=str(row["exchange"]) if pd.notna(row.get("exchange")) else None,
                    trade_id=str(row["trade_id"]) if pd.notna(row.get("trade_id")) else None,
                )
            )
        return records

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
            "net_pnl": "pnl_net",
            "gross_pnl": "pnl_price",
            "pnl_gross": "pnl_price",
            "risk": "risk_amount",
            "entry_stop_distance": "stop_distance",
            "stop_loss_distance": "stop_distance",
            "mae": "mae_price",
            "mfe": "mfe_price",
            "r_multiple": "r_multiple_net",
            "r": "r_multiple_net",
            "cost": "fees_paid",
        }
        rename_columns: dict[str, str] = {}
        existing = set(trades.columns)
        for source, target in rename_map.items():
            if source not in existing:
                continue
            if target in existing and source != target:
                continue
            rename_columns[source] = target
        normalized = trades.rename(columns=rename_columns).copy()

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
            "stop_distance": np.nan,
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
            "x_values": x_values,
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
        benchmark_overview = build_benchmark_overview_payload_from_metadata(
            run_metadata=run.metadata,
            strategy_series=run.equity,
            ts_column="ts",
            value_column="equity",
        )
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
        benchmark_payload = benchmark_overview["benchmark_comparison"]
        limitations = [
            "Parameter-topology metadata is absent, so overfitting diagnostics remain proxy-level.",
            "Execution and slippage assumptions are limited to trade-record fields.",
            "Regime context is trade-sequence only unless OHLCV context is supplied.",
        ]
        if benchmark_payload.get("limited"):
            benchmark_limitations = benchmark_payload.get("limitations") or []
            limitations.extend(str(item) for item in benchmark_limitations)
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
                "Provide benchmark config context in artifact metadata to enable benchmark-relative overview diagnostics.",
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
                "diagnostics_used": ["performance", "score", "equity_curve", "benchmark_comparison"],
                "figure_provenance": {
                    "equity_curve": figure_provenance,
                    "benchmark_overlay": "normalized_line_series_overlay",
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
            "benchmark_comparison": benchmark_payload,
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

    def _trade_distribution(
        self,
        trades: pd.DataFrame,
        *,
        semantic_capabilities: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        semantic_capabilities = semantic_capabilities or {}
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
        has_mae_mfe = bool(semantic_capabilities.get("can_build_mae_mfe_scatter", bool(mae_mfe_points)))
        has_duration = bool(semantic_capabilities.get("can_build_duration_distribution", bool(durations))) and bool(durations)

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
        abs_total_pnl = float(np.abs(pnl.to_numpy(dtype=float)).sum()) if trade_count else 0.0
        sorted_profit = sorted((float(value) for value in pnl_values if value > 0.0), reverse=True)
        top_trade_profit_contribution = (
            float(sorted_profit[0] / gross_profit)
            if sorted_profit and gross_profit > 0.0
            else None
        )
        top_5_trade_profit_contribution = (
            float(sum(sorted_profit[:5]) / gross_profit)
            if sorted_profit and gross_profit > 0.0
            else None
        )
        rare_trade_dependence = (
            "severe"
            if top_trade_profit_contribution is not None and top_trade_profit_contribution >= 0.5
            else "elevated"
            if top_5_trade_profit_contribution is not None and top_5_trade_profit_contribution >= 0.75
            else "moderate"
            if top_5_trade_profit_contribution is not None and top_5_trade_profit_contribution >= 0.5
            else "low"
        )
        pnl_without_top_winner = float(sum(pnl_values) - sorted_profit[0]) if sorted_profit else float(sum(pnl_values))
        edge_source = (
            "rare_winner_dominated"
            if rare_trade_dependence in {"severe", "elevated"}
            else "broad_trade_distribution"
            if win_count > 1 and gross_profit > 0.0
            else "insufficient_trade_dispersion"
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
        if rare_trade_dependence in {"severe", "elevated"}:
            cautions.append("Edge is materially dependent on a small number of winning trades.")

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
        if semantic_capabilities.get("can_build_histogram_from_r_multiples") and r_values:
            figures.append(
                self._figure_histogram(
                    figure_id="r_multiple_histogram",
                    title="R-Multiple Distribution",
                    x_label="r_multiple_net",
                    y_label="trade_count",
                    bins=self._histogram_bins(r_values, bins=min(max(int(np.sqrt(len(r_values))) + 4, 6), 20)),
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
                "top_trade_profit_contribution": top_trade_profit_contribution,
                "top_5_trade_profit_contribution": top_5_trade_profit_contribution,
                "rare_trade_dependence": rare_trade_dependence,
                "pnl_without_top_winner": pnl_without_top_winner,
                "edge_source": edge_source,
                "absolute_pnl_concentration_basis": abs_total_pnl,
            },
            "figures": figures,
            "interpretation": {
                "summary": summary,
                "positives": positives,
                "cautions": cautions,
                "shape_insights": shape_insights,
                "rare_trade_dependence": {
                    "classification": rare_trade_dependence,
                    "top_trade_profit_contribution": top_trade_profit_contribution,
                    "top_5_trade_profit_contribution": top_5_trade_profit_contribution,
                    "pnl_without_top_winner": pnl_without_top_winner,
                    "edge_source": edge_source,
                },
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
                    "r_multiple_available": bool(semantic_capabilities.get("can_build_histogram_from_r_multiples") and r_values),
                },
                "completeness_notes": limitations,
                "has_durations": has_duration,
                "has_mae_mfe": has_mae_mfe,
                "rare_trade_dependence": rare_trade_dependence,
                "edge_source": edge_source,
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
        semantic_capabilities: dict[str, bool] | None = None,
        fan_chart_paths: int = 50,
    ) -> dict[str, Any]:
        semantic_capabilities = semantic_capabilities or {}
        pnl = trades["pnl_net"].fillna(0.0).to_numpy(dtype=float)
        ruin_threshold_fraction = 0.5
        method = "bootstrap_iid_trade_pnl"
        if pnl.size == 0 or simulations <= 0:
            return {
                "methodology": {
                    "method": method,
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
                "probability_of_ruin": None,
                "ruin_threshold_equity": None,
                "ruin_threshold_fraction": ruin_threshold_fraction,
                "summary_metrics": {
                    "worst_drawdown": None,
                    "p95_drawdown": None,
                    "median_drawdown": None,
                    "p_ruin": None,
                },
                "figures": [],
                "drawdown_distribution": {"histogram_bins": [], "percentiles": {}},
                "interpretation": {
                    "summary": "Monte Carlo unavailable because trade outcomes or simulation count are missing.",
                    "risk_level": "unknown",
                    "positives": [],
                    "cautions": ["No simulation paths were generated."],
                },
                "warnings": ["No Monte Carlo simulations were produced."],
                "assumptions": ["Bootstrap simulation requires at least one trade return and simulations > 0."],
                "limitations": ["No valid trade outcomes or simulation paths were available."],
                "recommendations": ["Provide trade outcomes and simulation count > 0 to enable Monte Carlo diagnostics."],
                "metadata": {
                    "method": method,
                    "path_model": "iid_bootstrap_with_replacement",
                    "simulations": 0,
                    "trades_in_bootstrap": int(pnl.size),
                    "horizon_trades": int(pnl.size),
                    "ruin_threshold_fraction": ruin_threshold_fraction,
                    "capability_used": bool(semantic_capabilities.get("can_build_monte_carlo_paths", False)),
                },
            }

        rng = np.random.default_rng(seed)
        sampled = rng.choice(pnl, size=(simulations, pnl.size), replace=True)
        block_size = int(min(max(2, round(np.sqrt(float(pnl.size)))), max(2, pnl.size))) if pnl.size > 1 else 1
        block_paths: np.ndarray | None = None
        if pnl.size >= 4 and simulations > 0:
            block_rows: list[np.ndarray] = []
            for _ in range(simulations):
                values: list[float] = []
                while len(values) < pnl.size:
                    start = int(rng.integers(0, max(1, pnl.size - block_size + 1)))
                    values.extend(float(item) for item in pnl[start : start + block_size].tolist())
                block_rows.append(np.asarray(values[: pnl.size], dtype=float))
            block_sampled = np.vstack(block_rows)
            block_paths = float(initial_equity) + np.cumsum(block_sampled, axis=1)

        equity_paths = float(initial_equity) + np.cumsum(sampled, axis=1)
        running_peaks = np.maximum.accumulate(equity_paths, axis=1)
        drawdowns = np.where(running_peaks > 0, (equity_paths - running_peaks) / running_peaks, 0.0)
        max_drawdowns = drawdowns.min(axis=1)

        threshold_probs = {
            f"dd_{int(level * 100)}": float((max_drawdowns <= -float(level)).mean())
            for level in drawdown_levels
        }

        ruin_threshold_equity = float(initial_equity * ruin_threshold_fraction)
        probability_of_ruin = float((equity_paths.min(axis=1) <= ruin_threshold_equity).mean())

        drawdown_severity_pct = -max_drawdowns * 100.0
        quantiles = self._quantiles(max_drawdowns, points=(0.05, 0.5, 0.95))
        severity_quantiles = self._quantiles(drawdown_severity_pct, points=(0.5, 0.95))
        percentile_paths = np.quantile(equity_paths, [0.05, 0.25, 0.5, 0.75, 0.95], axis=0)
        worst_drawdown_pct = float(max_drawdowns.min() * 100.0)
        median_drawdown_pct = float(np.median(max_drawdowns) * 100.0)
        p95_drawdown_pct = float(np.quantile(max_drawdowns, 0.05) * 100.0)
        p5_drawdown_pct = float(np.quantile(max_drawdowns, 0.95) * 100.0)
        expected_drawdown_pct = float(max_drawdowns.mean() * 100.0)
        recovery_steps = np.argmax(equity_paths >= float(initial_equity), axis=1) + 1
        never_recovered = np.all(equity_paths < float(initial_equity), axis=1)
        recovery_steps = np.where(never_recovered, np.nan, recovery_steps.astype(float))
        recovery_success_rate = float(np.isfinite(recovery_steps).mean())
        serial_dependence = (
            float(np.corrcoef(pnl[:-1], pnl[1:])[0, 1])
            if pnl.size > 2 and np.std(pnl[:-1]) > 0.0 and np.std(pnl[1:]) > 0.0
            else None
        )
        block_bootstrap_metrics = None
        if block_paths is not None:
            block_peaks = np.maximum.accumulate(block_paths, axis=1)
            block_drawdowns = np.where(block_peaks > 0, (block_paths - block_peaks) / block_peaks, 0.0)
            block_max_drawdowns = block_drawdowns.min(axis=1)
            block_bootstrap_metrics = {
                "block_size": block_size,
                "worst_drawdown_pct": float(block_max_drawdowns.min() * 100.0),
                "p95_drawdown_pct": float(np.quantile(block_max_drawdowns, 0.05) * 100.0),
                "median_drawdown_pct": float(np.median(block_max_drawdowns) * 100.0),
                "simulation_model": "contiguous_block_bootstrap_trade_pnl",
            }

        return {
            "methodology": {
                "method": method,
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
            "drawdown_distribution": {
                "histogram_bins": self._histogram_bins(
                    [float(value) for value in drawdown_severity_pct.tolist()],
                    bins=18,
                ),
                "percentiles": {
                    "p5": p5_drawdown_pct,
                    "p50": median_drawdown_pct,
                    "p95": p95_drawdown_pct,
                    "worst": worst_drawdown_pct,
                },
            },
            "worst_drawdown_pct": worst_drawdown_pct,
            "median_drawdown_pct": median_drawdown_pct,
            "probability_by_drawdown_threshold": threshold_probs,
            "probability_of_ruin": probability_of_ruin,
            "ruin_threshold_equity": ruin_threshold_equity,
            "ruin_threshold_fraction": ruin_threshold_fraction,
            "summary_metrics": {
                "worst_drawdown": worst_drawdown_pct,
                "p95_drawdown": p95_drawdown_pct,
                "median_drawdown": median_drawdown_pct,
                "p_ruin": probability_of_ruin,
                "p5_drawdown": p5_drawdown_pct,
                "expected_drawdown": expected_drawdown_pct,
                "recovery_success_rate": recovery_success_rate,
                "recovery_median_trades": (
                    float(np.nanmedian(recovery_steps)) if np.isfinite(recovery_steps).any() else None
                ),
                "path_dispersion_terminal_equity_p10": float(np.quantile(equity_paths[:, -1], 0.1)),
                "path_dispersion_terminal_equity_p90": float(np.quantile(equity_paths[:, -1], 0.9)),
                "worst_simulated_drawdown_pct": worst_drawdown_pct,
                "median_drawdown_pct": float(quantiles["0.5"] * 100.0),
                "drawdown_p95_pct": float(quantiles["0.95"] * 100.0),
                "probability_of_ruin": probability_of_ruin,
                "ruin_threshold_equity": ruin_threshold_equity,
                "serial_dependence_lag1": serial_dependence,
                "block_bootstrap_p95_drawdown_pct": (
                    block_bootstrap_metrics["p95_drawdown_pct"] if block_bootstrap_metrics else None
                ),
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
                        "p5": [float(value) for value in percentile_paths[0].tolist()],
                        "p25": [float(value) for value in percentile_paths[1].tolist()],
                        "p50": [float(value) for value in percentile_paths[2].tolist()],
                        "p75": [float(value) for value in percentile_paths[3].tolist()],
                        "p95": [float(value) for value in percentile_paths[4].tolist()],
                    },
                },
                self._figure_histogram(
                    figure_id="drawdown_histogram",
                    title="Simulated Max Drawdown Distribution",
                    x_label="max_drawdown_severity_pct",
                    y_label="simulation_count",
                    bins=self._histogram_bins([float(value) for value in drawdown_severity_pct.tolist()], bins=16),
                ),
            ],
            "interpretation": {
                "summary": (
                    f"Bootstrap IID Monte Carlo over {simulations} paths shows median max drawdown "
                    f"{median_drawdown_pct:.2f}% and 95th-tail drawdown {p95_drawdown_pct:.2f}%."
                ),
                "risk_level": (
                    "high_tail_risk"
                    if p95_drawdown_pct <= -40.0
                    else "moderate_tail_risk"
                    if p95_drawdown_pct <= -25.0
                    else "contained_tail_risk"
                ),
                "positives": [
                    f"Median simulated drawdown remains {median_drawdown_pct:.2f}%.",
                    f"Recovery to initial equity occurred in {recovery_success_rate * 100.0:.1f}% of paths.",
                ],
                "cautions": [
                    f"Worst simulated drawdown reaches {worst_drawdown_pct:.2f}%.",
                    (
                        f"Estimated ruin probability is {probability_of_ruin:.2%} at "
                        f"{ruin_threshold_fraction:.0%} starting-equity threshold."
                    ),
                ],
            },
            "warnings": [],
            "assumptions": [
                "IID bootstrap resampling with replacement from realized trade-level net PnL.",
                "Simulation horizon equals observed trade count in the uploaded artifact.",
                "Ruin is defined as any path breaching 50% of starting equity.",
            ],
            "limitations": [
                "No serial dependence, volatility clustering, or regime-aware sequencing is modeled.",
                "Block bootstrap is reported as a secondary sensitivity check when enough trades exist; it is not a full regime-aware simulator.",
                "No liquidity, slippage amplification, or execution-gapping stress is injected beyond supplied trade PnL.",
                "Baseline Monte Carlo is unconditional IID bootstrap; conditional/context-aware simulation is not implemented here.",
            ],
            "recommendations": [
                "Use p95 drawdown and ruin probability as sizing guardrails for survivability decisions.",
                "Reduce position sizing if tail drawdown and ruin probabilities exceed risk policy.",
                "Add regime labels/OHLCV context to support future conditional or block-bootstrap Monte Carlo extensions.",
            ],
            "metadata": {
                "method": method,
                "path_model": "iid_bootstrap_with_replacement",
                "simulations": int(simulations),
                "num_paths": int(simulations),
                "trades_in_bootstrap": int(pnl.size),
                "horizon_trades": int(pnl.size),
                "ruin_threshold_fraction": ruin_threshold_fraction,
                "ruin_threshold_equity": ruin_threshold_equity,
                "figure_payload_mode": "percentile_bands_with_optional_sample_paths",
                "fan_chart_band_percentiles": [5, 25, 50, 75, 95],
                "drawdown_distribution_mode": "max_drawdown_histogram_and_percentiles",
                "compute_simplifications": ["iid_resampling", "fixed_horizon_equal_to_observed_trade_count"],
                "serial_dependence_lag1": serial_dependence,
                "block_bootstrap": block_bootstrap_metrics,
                "regime_conditioned_mode_available": bool(semantic_capabilities.get("can_build_regime_analysis", False)),
                "capability_used": bool(semantic_capabilities.get("can_build_monte_carlo_paths", True)),
            },
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

    def _parameter_stability_from_parameter_sweep(
        self,
        sweep: ParameterSweepInput,
        *,
        metric: str = "ev_net",
    ) -> dict[str, Any]:
        if len(sweep.runs) < 2:
            raise IngestionError("parameter_sweep requires at least two runs for stability analysis.")

        points: list[dict[str, Any]] = []
        metric_values: list[float] = []
        combinations: set[tuple[tuple[str, int | float | str | bool], ...]] = set()
        for run in sweep.runs:
            key = tuple((name, run.params[name]) for name in sweep.parameter_names)
            combinations.add(key)
            value = None
            if run.summary is not None and metric in run.summary:
                value = pd.to_numeric(run.summary.get(metric), errors="coerce")
            if pd.isna(value):
                pnls = [trade.pnl for trade in run.trades if trade.pnl is not None]
                value = float(np.mean(pnls)) if pnls else np.nan
            if pd.isna(value):
                raise IngestionError(
                    f"Run '{run.run_id}' missing numeric '{metric}' summary and has no trade pnl records."
                )
            metric_value = float(value)
            metric_values.append(metric_value)
            point = {"run_id": run.run_id, "value": metric_value, "params": dict(run.params)}
            if sweep.parameter_names:
                point["x"] = run.params[sweep.parameter_names[0]]
            if len(sweep.parameter_names) > 1:
                point["y"] = run.params[sweep.parameter_names[1]]
            points.append(point)

        if len(combinations) < 2:
            raise IngestionError("parameter_sweep requires multiple unique parameter combinations.")

        metric_series = pd.Series(metric_values, dtype=float)
        x_key = sweep.parameter_names[0]
        y_key = sweep.parameter_names[1] if len(sweep.parameter_names) > 1 else "run_id"
        if len(sweep.parameter_names) == 1:
            for point in points:
                point["y"] = point["run_id"]
        payload = self._parameter_stability_common(
            metric_series=metric_series,
            heatmap=points,
            x_key=x_key,
            y_key=y_key,
        )
        payload["metadata"]["parameter_names"] = list(sweep.parameter_names)
        payload["metadata"]["dimensions"] = len(sweep.parameter_names)
        payload["metadata"]["runs"] = len(sweep.runs)
        payload["status"] = "parameter_sweep"
        return payload

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
        lower_quartile = float(metric_series.quantile(0.25))
        robust_region_ratio = float((metric_series >= median).mean())
        fragility = 1.0 if peak == 0 else max(0.0, min(1.0, 1.0 - (median / peak)))
        cliff_risk = (
            float((peak - lower_quartile) / abs(peak))
            if peak != 0.0
            else 1.0
        )
        topology = (
            "broad_plateau" if plateau_ratio >= 0.25 and cliff_risk < 0.5 else
            "local_optimum" if plateau_ratio < 0.15 and cliff_risk >= 0.5 else
            "mixed_surface"
        )
        score = max(0.0, min(100.0, (plateau_ratio * 60.0) + ((1.0 - fragility) * 40.0)))
        return {
            "summary_metrics": {
                "stability_score": score,
                "plateau_ratio": plateau_ratio,
                "peak_fragility": fragility,
                "robust_region_ratio": robust_region_ratio,
                "cliff_risk": cliff_risk,
                "topology": topology,
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
            "metadata": {
                "grid_points": int(len(metric_series)),
                "axes": {"x": x_key, "y": y_key, "value": "metric"},
                "topology": topology,
                "robust_region_ratio": robust_region_ratio,
                "cliff_risk": cliff_risk,
            },
            "stability_score": score,
            "plateau_ratio": plateau_ratio,
            "peak_fragility": fragility,
            "robust_region_ratio": robust_region_ratio,
            "cliff_risk": cliff_risk,
            "topology": topology,
            "heatmap": heatmap,
            "axes": {"x": x_key, "y": y_key, "value": "metric"},
        }

    def _execution_sensitivity(
        self,
        trades: pd.DataFrame,
        *,
        semantic_capabilities: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        semantic_capabilities = semantic_capabilities or {}
        base = trades["pnl_net"].fillna(0.0)
        fees = trades["fees_paid"].fillna(0.0)
        slippage = trades["slippage"].fillna(0.0)
        spread = trades["spread"].fillna(0.0)
        notional = (
            trades["entry_price"].abs().fillna(0.0) * trades["quantity"].abs().fillna(0.0)
        ).fillna(0.0)
        resolved_notional = notional.where(notional > 0.0, np.nan)
        risk_amount = trades["risk_amount"].abs().replace(0.0, np.nan) if "risk_amount" in trades.columns else pd.Series(np.nan, index=trades.index)

        def _profit_factor(pnl_series: pd.Series) -> float | None:
            gross_profit = float(pnl_series[pnl_series > 0.0].sum())
            gross_loss_abs = abs(float(pnl_series[pnl_series < 0.0].sum()))
            if gross_loss_abs <= 0.0:
                return None
            return float(gross_profit / gross_loss_abs)

        scenarios = [
            {
                "name": "baseline",
                "spread_rate": 0.0,
                "slippage_rate": 0.0,
                "fee_rate": 0.0,
                "severity": 0,
            },
            {
                "name": "moderate_stress",
                "spread_rate": 0.0005,
                "slippage_rate": 0.0005,
                "fee_rate": 0.0004,
                "severity": 1,
            },
            {
                "name": "high_stress",
                "spread_rate": 0.0015,
                "slippage_rate": 0.0015,
                "fee_rate": 0.0007,
                "severity": 2,
            },
            {
                "name": "extreme_stress",
                "spread_rate": 0.0030,
                "slippage_rate": 0.0030,
                "fee_rate": 0.0010,
                "severity": 3,
            },
        ]

        baseline_expectancy = float(base.mean()) if len(base) else 0.0
        baseline_win_rate = float((base > 0.0).mean()) if len(base) else 0.0
        baseline_profit_factor = _profit_factor(base)
        scenario_rows: list[dict[str, Any]] = []
        for scenario in scenarios:
            spread_cost = resolved_notional.fillna(0.0) * float(scenario["spread_rate"])
            slippage_cost = resolved_notional.fillna(0.0) * float(scenario["slippage_rate"])
            fee_cost = resolved_notional.fillna(0.0) * float(scenario["fee_rate"])
            stressed_trade_pnl = base - slippage_cost - spread_cost - fee_cost
            expectancy = float(stressed_trade_pnl.mean()) if len(stressed_trade_pnl) else 0.0
            win_rate = float((stressed_trade_pnl > 0.0).mean()) if len(stressed_trade_pnl) else 0.0
            profit_factor = _profit_factor(stressed_trade_pnl)
            if baseline_expectancy == 0.0:
                edge_decay_pct = 0.0 if expectancy >= 0.0 else 100.0
            else:
                edge_decay_pct = max(0.0, ((baseline_expectancy - expectancy) / abs(baseline_expectancy)) * 100.0)
            if scenario["name"] == "baseline":
                classification = "baseline"
            elif expectancy <= 0.0:
                classification = "negative"
            elif edge_decay_pct >= 70.0:
                classification = "fragile"
            else:
                classification = "survives"
            average_r = None
            if risk_amount.notna().any():
                average_r = float((stressed_trade_pnl / risk_amount).replace([np.inf, -np.inf], np.nan).dropna().mean())
            scenario_rows.append(
                {
                    "name": scenario["name"],
                    "severity": int(scenario["severity"]),
                    "spread_assumption": {
                        "rate": float(scenario["spread_rate"]),
                        "pct": float(scenario["spread_rate"] * 100.0),
                        "bps": float(scenario["spread_rate"] * 10_000.0),
                        "formula": "spread_cost = entry_price * quantity * spread_rate",
                    },
                    "slippage_assumption": {
                        "rate": float(scenario["slippage_rate"]),
                        "pct": float(scenario["slippage_rate"] * 100.0),
                        "bps": float(scenario["slippage_rate"] * 10_000.0),
                        "formula": "slippage_cost = entry_price * quantity * slippage_rate",
                    },
                    "fee_assumption": {
                        "rate": float(scenario["fee_rate"]),
                        "pct": float(scenario["fee_rate"] * 100.0),
                        "bps": float(scenario["fee_rate"] * 10_000.0),
                        "formula": "fee_cost = entry_price * quantity * taker_fee_rate",
                    },
                    "spread_bps": float(scenario["spread_rate"] * 10_000.0),
                    "slippage_bps": float(scenario["slippage_rate"] * 10_000.0),
                    "fee_bps": float(scenario["fee_rate"] * 10_000.0),
                    "expectancy": expectancy,
                    "win_rate": win_rate,
                    "profit_factor": profit_factor,
                    "average_r": average_r,
                    "edge_decay_pct": float(edge_decay_pct),
                    "classification": classification,
                }
            )

        stressed_row = min(scenario_rows[1:], key=lambda row: row["expectancy"]) if len(scenario_rows) > 1 else scenario_rows[0]
        stressed_expectancy = float(stressed_row["expectancy"])
        stressed_win_rate = float(stressed_row["win_rate"])
        stressed_profit_factor = float(stressed_row["profit_factor"]) if stressed_row["profit_factor"] is not None else None
        edge_decay_abs = float(baseline_expectancy - stressed_expectancy)
        edge_decay_pct = (
            float(max(0.0, (edge_decay_abs / abs(baseline_expectancy)) * 100.0))
            if baseline_expectancy != 0.0
            else (100.0 if stressed_expectancy < 0.0 else 0.0)
        )
        total_bps = [row["spread_bps"] + row["slippage_bps"] + row["fee_bps"] for row in scenario_rows]
        break_even_cost_threshold = None
        for idx in range(1, len(scenario_rows)):
            prev_row = scenario_rows[idx - 1]
            next_row = scenario_rows[idx]
            prev_exp = float(prev_row["expectancy"])
            next_exp = float(next_row["expectancy"])
            if prev_exp > 0.0 >= next_exp:
                prev_bps = float(total_bps[idx - 1])
                next_bps = float(total_bps[idx])
                slope = next_exp - prev_exp
                if slope == 0.0:
                    break_even_cost_threshold = next_bps
                else:
                    frac = (0.0 - prev_exp) / slope
                    break_even_cost_threshold = prev_bps + ((next_bps - prev_bps) * frac)
                break
        if break_even_cost_threshold is None and scenario_rows and scenario_rows[-1]["expectancy"] > 0.0:
            break_even_cost_threshold = float(total_bps[-1])

        resilience = 100.0 - min(100.0, edge_decay_pct)
        richer_cost_fields = bool((spread.abs() > 0).any() or (slippage.abs() > 0).any() or (fees.abs() > 0).any())
        has_notional = bool(resolved_notional.notna().any())
        execution_model_type = "enhanced" if (has_notional and richer_cost_fields) else "baseline"
        has_broker_export = bool(semantic_capabilities.get("has_broker_export", False))
        execution_fantasy_triggers: list[str] = []
        if not has_broker_export:
            execution_fantasy_triggers.append("broker_or_fill_export_missing")
        if not richer_cost_fields:
            execution_fantasy_triggers.append("explicit_cost_fields_sparse")
        if break_even_cost_threshold is not None and break_even_cost_threshold <= 20.0:
            execution_fantasy_triggers.append("edge_dies_under_small_cost_increase")
        if stressed_expectancy <= 0.0:
            execution_fantasy_triggers.append("stressed_expectancy_negative")
        broker_fill_audit = {
            "broker_export_present": has_broker_export,
            "fill_quality_model": "broker_export_proxy" if has_broker_export else "not_available",
            "anomalies": [] if has_broker_export else ["No broker/fill export supplied; cannot verify fills, venue fees, partial fills, or timestamp realism."],
            "confidence": "limited" if has_broker_export else "unsupported",
        }
        dominant = {
            "spread_bps": float(spread.abs().mean()),
            "slippage_bps": float(slippage.abs().mean()),
            "fee_bps": float(fees.abs().mean()),
        }
        dominant_dimension = max(dominant.items(), key=lambda item: item[1])[0] if dominant else "fee_bps"
        interpretation = {
            "summary": (
                f"Expectancy decays from {baseline_expectancy:.4f} to {stressed_expectancy:.4f} "
                f"({edge_decay_pct:.1f}% decay) under stressed execution assumptions."
            ),
            "positives": [
                "Execution sensitivity is computed directly from trade records without OHLCV dependence.",
                "Scenario matrix is deterministic and fully declared for auditability and UI comparison.",
            ],
            "cautions": [
                f"Most impactful observed execution-cost dimension is {dominant_dimension}.",
                f"Worst stressed scenario '{stressed_row['name']}' is classified as '{stressed_row['classification']}'.",
            ],
        }

        recommendations = [
            "Deploy only if expectancy remains positive through at least moderate stress scenarios.",
            "Calibrate deterministic fee/slippage/spread schedules to your venue before production sizing.",
            "Treat this diagnostic as conservative proxy stress; it does not model market impact or latency.",
        ]
        if stressed_row["classification"] in {"fragile", "negative"}:
            recommendations.insert(0, "Reduce sizing or tighten trade selection when edge is fragile under execution stress.")

        assumptions = [
            "Deterministic trade-level proxy model: pnl_net_adjusted = pnl_net_original - slippage_cost - spread_cost - fee_cost.",
            "Notional proxy is strictly entry_price * quantity from persisted trade fields.",
            "Slippage and spread use identical schedules per scenario: baseline 0.00%, moderate 0.05%, high 0.15%, extreme 0.30%.",
            "Deterministic taker-fee schedule: baseline 0.00%, moderate 0.04%, high 0.07%, extreme 0.10%.",
            "No stochastic simulation, OHLCV-derived regime context, or hidden parameters are used in execution sensitivity.",
            f"Execution assumptions are {'augmented with observed cost fields for metadata context' if richer_cost_fields else 'pure deterministic proxies because explicit cost fields are sparse'} in this run.",
        ]

        return {
            "summary_metrics": {
                "baseline_expectancy": baseline_expectancy,
                "stressed_expectancy": stressed_expectancy,
                "edge_decay_abs": edge_decay_abs,
                "edge_decay_pct": edge_decay_pct,
                "baseline_win_rate": baseline_win_rate,
                "stressed_win_rate": stressed_win_rate,
                "baseline_profit_factor": baseline_profit_factor,
                "stressed_profit_factor": stressed_profit_factor,
                "break_even_cost_threshold_bps": break_even_cost_threshold,
                "cost_kill_threshold_bps": break_even_cost_threshold,
                "execution_fantasy_trigger_count": len(execution_fantasy_triggers),
                "stressed_scenario": stressed_row["name"],
                "baseline_ev_net": baseline_expectancy,
                "execution_resilience_score": resilience,
                "break_even_cost_multiplier": None,
            },
            "figures": [
                self._figure_line_series(
                    figure_id="execution_expectancy_decay",
                    title="Execution Expectancy Decay by Scenario",
                    x_label="scenario",
                    y_label="expectancy",
                    x_values=[row["name"] for row in scenario_rows],
                    series=[{"name": "expectancy", "values": [row["expectancy"] for row in scenario_rows]}],
                )
            ],
            "scenarios": scenario_rows,
            "interpretation": interpretation,
            "warnings": [],
            "assumptions": assumptions,
            "limitations": [
                "No order-book simulation is performed.",
                "No market-impact model is included.",
                "No latency modeling is included.",
                "No venue-specific routing/execution model is included.",
                "Proxy costs are based on entry notional only and do not account for intrabar spread dynamics.",
            ],
            "recommendations": recommendations,
            "metadata": {
                "scenario_count": len(scenario_rows),
                "scenario_levels": [row["name"] for row in scenario_rows],
                "stress_shape": "discrete",
                "execution_model_type": execution_model_type,
                "methodology": "deterministic_trade_notional_proxy",
                "deterministic_proxy": {
                    "notional_formula": "abs(entry_price) * abs(quantity)",
                    "pnl_adjustment_formula": "pnl_net_adjusted = pnl_net_original - slippage_cost - spread_cost - fee_cost",
                    "slippage_schedule_rate": {
                        "baseline": 0.0000,
                        "moderate_stress": 0.0005,
                        "high_stress": 0.0015,
                        "extreme_stress": 0.0030,
                    },
                    "spread_schedule_rate": {
                        "baseline": 0.0000,
                        "moderate_stress": 0.0005,
                        "high_stress": 0.0015,
                        "extreme_stress": 0.0030,
                    },
                    "fee_schedule_rate": {
                        "baseline": 0.0000,
                        "moderate_stress": 0.0004,
                        "high_stress": 0.0007,
                        "extreme_stress": 0.0010,
                    },
                    "slippage_schedule_pct": {
                        "baseline": 0.00,
                        "moderate_stress": 0.05,
                        "high_stress": 0.15,
                        "extreme_stress": 0.30,
                    },
                    "spread_schedule_pct": {
                        "baseline": 0.00,
                        "moderate_stress": 0.05,
                        "high_stress": 0.15,
                        "extreme_stress": 0.30,
                    },
                    "taker_fee_schedule_pct": {
                        "baseline": 0.00,
                        "moderate_stress": 0.04,
                        "high_stress": 0.07,
                        "extreme_stress": 0.10,
                    },
                },
                "dominant_cost_dimension": dominant_dimension,
                "broker_fill_audit": broker_fill_audit,
                "execution_fantasy_triggers": execution_fantasy_triggers,
                "cost_kill_threshold_bps": break_even_cost_threshold,
                "capability_used": bool(semantic_capabilities.get("can_build_execution_sensitivity_baseline", True)),
                "cost_drag_supported": bool(semantic_capabilities.get("can_build_cost_drag_summary", False)),
                "uses_ohlcv": False,
                "stochastic_modeling": False,
            },
            "baseline_ev_net": baseline_expectancy,
            "execution_resilience_score": resilience,
            "break_even_cost_multiplier": None,
            "broker_fill_audit": broker_fill_audit,
            "execution_fantasy_triggers": execution_fantasy_triggers,
            "cost_kill_threshold_bps": break_even_cost_threshold,
        }

    def _regime_analysis(self, trades: pd.DataFrame, *, ohlcv: pd.DataFrame | None = None) -> dict[str, Any]:
        if ohlcv is None or ohlcv.empty:
            return {
                "status": "unavailable",
                "summary_metrics": {},
                "figures": [],
                "regime_metrics": [],
                "interpretation": {
                    "summary": "Regime analysis unavailable because OHLCV market context was not supplied.",
                    "dominant_regime": None,
                    "weak_regime": None,
                    "classification": "unavailable",
                },
                "warnings": [],
                "assumptions": ["True regime analysis requires trade data plus OHLCV (or equivalent market context)."],
                "limitations": ["No OHLCV context was provided; trade-only regime proxy mode is intentionally disabled."],
                "recommendations": ["Provide OHLCV bars aligned to trade timestamps to enable regime classification."],
                "metadata": {
                    "regime_definition": "trend_volatility_quadrant",
                    "classification_method": "ma_slope_plus_atr_threshold",
                    "number_of_regimes": 4,
                    "parameters": {},
                },
                "regime_consistency_score": 0.0,
            }

        bars = self._classify_market_regimes(ohlcv)
        trade_regimes = self._map_trades_to_regimes(trades=trades, classified_bars=bars)
        regime_metrics = self._compute_regime_metrics(trade_regimes)

        if not regime_metrics:
            return {
                "status": "limited",
                "summary_metrics": {},
                "figures": [],
                "regime_metrics": [],
                "interpretation": {
                    "summary": "OHLCV was supplied, but no trades mapped to classified bars.",
                    "dominant_regime": None,
                    "weak_regime": None,
                    "classification": "fragile",
                },
                "warnings": [],
                "assumptions": ["Trades are mapped to the most recent classified OHLCV bar using backward asof matching."],
                "limitations": ["No trade-to-regime mappings were found in the overlapping timestamp window."],
                "recommendations": ["Align OHLCV timestamps and trade entry timestamps within the same market clock."],
                "metadata": {
                    "regime_definition": "trend_volatility_quadrant",
                    "classification_method": "ma_slope_plus_atr_threshold",
                    "number_of_regimes": 4,
                    "parameters": {"trade_mapping": "merge_asof_backward"},
                    "mapped_trade_count": 0,
                },
                "regime_consistency_score": 0.0,
            }

        expectancy_values = [float(row["expectancy"]) for row in regime_metrics]
        counts = [int(row["trade_count"]) for row in regime_metrics]
        best = max(regime_metrics, key=lambda row: float(row["expectancy"]))
        worst = min(regime_metrics, key=lambda row: float(row["expectancy"]))
        dominant = max(regime_metrics, key=lambda row: int(row["trade_count"]))
        dispersion = float(np.var(expectancy_values)) if expectancy_values else 0.0
        consistency = float(max(0.0, min(100.0, 100.0 - (dispersion * 100.0))))
        thin_regimes = [str(row["regime"]) for row in regime_metrics if int(row["trade_count"]) < 10]
        adverse_regime_decay = (
            float(float(best["expectancy"]) - float(worst["expectancy"]))
            if best and worst
            else 0.0
        )
        regime_classification = "regime-agnostic"
        if expectancy_values and (max(expectancy_values) - min(expectancy_values)) > 0.15:
            regime_classification = "regime-dependent"
        if float(worst["expectancy"]) < 0.0 and float(best["expectancy"]) <= 0.0:
            regime_classification = "fragile"

        return {
            "status": "available",
            "summary_metrics": {
                "best_regime": str(best["regime"]),
                "worst_regime": str(worst["regime"]),
                "dominant_regime": str(dominant["regime"]),
                "regime_dispersion": dispersion,
                "regime_consistency_score": consistency,
                "regime_count_observed": len(regime_metrics),
                "mapped_trade_count": int(sum(counts)),
                "thin_regime_count": len(thin_regimes),
                "adverse_regime_decay": adverse_regime_decay,
            },
            "figures": [
                {
                    "id": "regime_expectancy_bars",
                    "type": "bar_groups",
                    "title": "Expectancy by Regime",
                    "x_label": "regime",
                    "y_label": "expectancy",
                    "groups": [
                        {"label": str(row["regime"]), "value": float(row["expectancy"])}
                        for row in sorted(regime_metrics, key=lambda row: str(row["regime"]))
                    ],
                }
            ],
            "regime_metrics": regime_metrics,
            "interpretation": {
                "summary": (
                    f"Best regime is {best['regime']} (expectancy={best['expectancy']:.4f}); "
                    f"weakest regime is {worst['regime']} (expectancy={worst['expectancy']:.4f})."
                ),
                "dominant_regime": str(dominant["regime"]),
                "weak_regime": str(worst["regime"]),
                "classification": regime_classification,
            },
            "warnings": [
                f"Thin regime sample: {name} has fewer than 10 mapped trades."
                for name in thin_regimes
            ],
            "assumptions": [
                "Trend is defined from EMA slope and close-vs-EMA context.",
                "Volatility is defined from ATR percentage relative to its rolling median.",
                "Trade regime mapping uses nearest prior bar (merge_asof backward).",
                "Baseline regime taxonomy combines trend/range with high/low volatility quadrants.",
            ],
            "limitations": [
                "Regime classifier is a simplified technical baseline and does not include microstructure states.",
                "Macro, cross-asset, and event regimes are not modeled.",
                "No forward regime prediction is produced by this diagnostic.",
            ],
            "recommendations": [
                f"Deploy preferentially in {best['regime']} and avoid {worst['regime']} until edge stabilizes.",
                "Use this regime signal as a pre-trade filter in strategy routing.",
                "Improve classification with richer features (realized vol term structure, liquidity, macro tags).",
            ],
            "metadata": {
                "regime_definition": "trend_volatility_quadrant",
                "classification_method": "ma_slope_plus_atr_threshold",
                "number_of_regimes": 4,
                "parameters": {
                    "trend_ma_span": 20,
                    "trend_slope_lookback": 5,
                    "atr_window": 14,
                    "atr_vol_threshold": 1.0,
                    "range_slope_epsilon": 0.0002,
                },
                "mapped_trade_count": int(sum(counts)),
                "ohlcv_bar_count": int(len(bars)),
                "thin_regimes": thin_regimes,
                "adverse_regime_decay": adverse_regime_decay,
            },
            "regime_consistency_score": consistency,
        }

    def _normalize_ohlcv(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        frame = pd.DataFrame(rows)
        rename_map = {"timestamp": "ts", "time": "ts", "datetime": "ts"}
        frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns}).copy()
        required = {"ts", "open", "high", "low", "close"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise IngestionError(f"OHLCV context missing required columns: {missing}")
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True, errors="coerce")
        if frame["ts"].isna().any():
            raise IngestionError("OHLCV context has invalid timestamps; use ISO-8601 values.")
        for column in ("open", "high", "low", "close", "volume"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        normalized = frame.sort_values("ts").dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
        if normalized.empty:
            raise IngestionError("OHLCV context has no valid rows after normalization.")
        return normalized

    def _classify_market_regimes(self, ohlcv: pd.DataFrame) -> pd.DataFrame:
        bars = ohlcv.copy().sort_values("ts").reset_index(drop=True)
        bars["ema"] = bars["close"].ewm(span=20, adjust=False).mean()
        bars["ema_slope"] = (bars["ema"] - bars["ema"].shift(5)) / bars["ema"].shift(5).replace(0.0, np.nan)
        prev_close = bars["close"].shift(1)
        tr_components = pd.concat(
            [
                bars["high"] - bars["low"],
                (bars["high"] - prev_close).abs(),
                (bars["low"] - prev_close).abs(),
            ],
            axis=1,
        )
        bars["atr"] = tr_components.max(axis=1).rolling(14, min_periods=5).mean()
        bars["atr_pct"] = bars["atr"] / bars["close"].replace(0.0, np.nan)
        bars["atr_median"] = bars["atr_pct"].rolling(50, min_periods=10).median()

        slope = bars["ema_slope"].fillna(0.0)
        close_vs_ema = (bars["close"] - bars["ema"]) / bars["ema"].replace(0.0, np.nan)
        close_vs_ema = close_vs_ema.fillna(0.0)
        trend_state = np.where(
            slope > 0.0002,
            "uptrend",
            np.where(slope < -0.0002, "downtrend", "range"),
        )
        trend_state = np.where((np.abs(slope) <= 0.0002) & (np.abs(close_vs_ema) <= 0.001), "range", trend_state)
        vol_ratio = bars["atr_pct"] / bars["atr_median"].replace(0.0, np.nan)
        vol_state = np.where(vol_ratio.fillna(0.0) >= 1.0, "high_vol", "low_vol")
        trend_bucket = np.where(trend_state == "range", "range", "trend")
        bars["regime"] = [f"{v}_{t}" for v, t in zip(vol_state, trend_bucket, strict=False)]
        return bars[["ts", "regime", "ema_slope", "atr_pct", "atr_median"]]

    def _map_trades_to_regimes(self, *, trades: pd.DataFrame, classified_bars: pd.DataFrame) -> pd.DataFrame:
        mapping = pd.merge_asof(
            trades.sort_values("entry_ts"),
            classified_bars.sort_values("ts"),
            left_on="entry_ts",
            right_on="ts",
            direction="backward",
        )
        mapping = mapping.dropna(subset=["regime"]).copy()
        return mapping

    def _compute_regime_metrics(self, trade_regimes: pd.DataFrame) -> list[dict[str, Any]]:
        if trade_regimes.empty:
            return []
        rows: list[dict[str, Any]] = []
        for regime, group in trade_regimes.groupby("regime"):
            pnl = group["pnl_net"].fillna(0.0).astype(float)
            wins = (pnl > 0).mean() if len(pnl) else 0.0
            equity = pnl.cumsum()
            peak = equity.cummax()
            dd = (equity - peak).min() if len(equity) else 0.0
            rows.append(
                {
                    "regime": str(regime),
                    "trade_count": int(len(group)),
                    "expectancy": float(pnl.mean()) if len(pnl) else 0.0,
                    "win_rate": float(wins),
                    "drawdown": float(dd),
                }
            )
        return sorted(rows, key=lambda row: row["regime"])

    def _risk_of_ruin(
        self,
        monte_carlo: dict[str, Any],
        *,
        trades: pd.DataFrame,
        account_size: float,
        explicit_account_size: float | None,
        risk_per_trade_pct: float | None,
        semantic_capabilities: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        semantic_capabilities = semantic_capabilities or {}
        pnl = trades["pnl_net"].fillna(0.0).to_numpy(dtype=float)
        levels = monte_carlo.get("probability_by_drawdown_threshold", {})
        mc_summary = monte_carlo.get("summary_metrics", {})
        simulations = int(monte_carlo.get("simulations", 0) or 0)
        monte_carlo_linked = simulations > 0 and pnl.size > 0
        ruin_threshold_fraction = float(monte_carlo.get("ruin_threshold_fraction", 0.5))
        ruin_threshold_equity = float(monte_carlo.get("ruin_threshold_equity", account_size * ruin_threshold_fraction))
        expected_worst_drawdown = mc_summary.get("expected_drawdown")
        expected_stress_drawdown = float(expected_worst_drawdown) if expected_worst_drawdown is not None else None
        mc_probability_of_ruin = mc_summary.get("probability_of_ruin", monte_carlo.get("probability_of_ruin"))
        p95_drawdown_pct = mc_summary.get("p95_drawdown", mc_summary.get("drawdown_p95_pct"))
        worst_simulated_drawdown_pct = mc_summary.get(
            "worst_simulated_drawdown_pct",
            mc_summary.get("worst_drawdown", monte_carlo.get("worst_drawdown_pct")),
        )
        median_simulated_drawdown_pct = mc_summary.get(
            "median_drawdown_pct",
            mc_summary.get("median_drawdown", monte_carlo.get("median_drawdown_pct")),
        )

        def _streak_lengths(mask: np.ndarray) -> list[int]:
            lengths: list[int] = []
            run = 0
            for value in mask.tolist():
                if bool(value):
                    run += 1
                elif run > 0:
                    lengths.append(run)
                    run = 0
            if run > 0:
                lengths.append(run)
            return lengths

        def _loss_streak_payload(values: np.ndarray) -> dict[str, Any]:
            losses = values < 0.0
            wins = values > 0.0
            loss_lengths = _streak_lengths(losses)
            win_lengths = _streak_lengths(wins)
            max_losses = int(max(loss_lengths) if loss_lengths else 0)
            max_wins = int(max(win_lengths) if win_lengths else 0)
            avg_loss_streak = float(np.mean(loss_lengths)) if loss_lengths else 0.0
            median_loss_streak = float(np.median(loss_lengths)) if loss_lengths else 0.0
            avg_win_streak = float(np.mean(win_lengths)) if win_lengths else 0.0
            median_win_streak = float(np.median(win_lengths)) if win_lengths else 0.0

            longest_loss_pnl = 0.0
            if max_losses > 0:
                running_len = 0
                running_sum = 0.0
                streak_sums: list[float] = []
                for point in values.tolist():
                    if point < 0.0:
                        running_len += 1
                        running_sum += float(point)
                    else:
                        if running_len == max_losses:
                            streak_sums.append(running_sum)
                        running_len = 0
                        running_sum = 0.0
                if running_len == max_losses:
                    streak_sums.append(running_sum)
                longest_loss_pnl = float(min(streak_sums)) if streak_sums else 0.0

            unique_lengths, counts = (
                np.unique(np.asarray(loss_lengths, dtype=int), return_counts=True) if loss_lengths else ([], [])
            )
            distribution = [
                {"streak_length": int(length), "count": int(count)}
                for length, count in zip(unique_lengths, counts, strict=False)
            ]

            largest_single_loss = float(values.min()) if values.size else 0.0
            return {
                "max_consecutive_losses": max_losses,
                "max_consecutive_wins": max_wins,
                "average_losing_streak": avg_loss_streak,
                "median_losing_streak": median_loss_streak,
                "average_winning_streak": avg_win_streak,
                "median_winning_streak": median_win_streak,
                "longest_losing_streak_pnl": longest_loss_pnl,
                "loss_streak_distribution": distribution,
                "largest_single_loss": largest_single_loss,
            }

        def _historical_drawdown(values: np.ndarray, base_capital: float) -> dict[str, Any]:
            if values.size == 0 or base_capital <= 0:
                return {
                    "worst_drawdown_pct": 0.0,
                    "worst_drawdown_duration_trades": 0,
                    "recovery_trades_after_worst_drawdown": None,
                }
            equity = base_capital + np.cumsum(values)
            full_equity = np.concatenate([[base_capital], equity])
            running_peak = np.maximum.accumulate(full_equity)
            drawdowns = np.where(running_peak > 0, (full_equity - running_peak) / running_peak, 0.0)
            worst_idx = int(np.argmin(drawdowns))
            worst_pct = float(drawdowns[worst_idx] * 100.0)

            longest_duration = 0
            active_start: int | None = None
            for idx in range(1, full_equity.size):
                if full_equity[idx] < running_peak[idx]:
                    if active_start is None:
                        active_start = idx
                elif active_start is not None:
                    longest_duration = max(longest_duration, idx - active_start)
                    active_start = None
            if active_start is not None:
                longest_duration = max(longest_duration, (full_equity.size - 1) - active_start + 1)

            recovery_trades: int | None = None
            if worst_idx > 0:
                prior_peak = running_peak[worst_idx]
                recovery_slice = np.where(full_equity[worst_idx:] >= prior_peak)[0]
                if recovery_slice.size > 0 and int(recovery_slice[0]) > 0:
                    recovery_trades = int(recovery_slice[0])

            return {
                "worst_drawdown_pct": worst_pct,
                "worst_drawdown_duration_trades": int(longest_duration),
                "recovery_trades_after_worst_drawdown": recovery_trades,
            }

        streak_stats = _loss_streak_payload(pnl)
        historical_drawdown_stats = _historical_drawdown(pnl, float(account_size))

        required_missing: list[str] = []
        if explicit_account_size is None:
            required_missing.append("account_size")
        if risk_per_trade_pct is None:
            required_missing.append("risk_per_trade_pct")

        assumptions = [
            (
                f"account_size={float(explicit_account_size):.2f}"
                if explicit_account_size is not None
                else "account_size not explicitly provided (Monte Carlo seed equity fallback was used)."
            ),
            (
                f"risk_per_trade_pct={float(risk_per_trade_pct):.4f}"
                if risk_per_trade_pct is not None
                else "risk_per_trade_pct not explicitly provided."
            ),
            "sizing_model=fixed_fractional (trade PnL normalized by configured risk-per-trade capital).",
            "compounding_model=fixed-notional within each simulation horizon.",
            "trade sequencing assumes IID bootstrap with replacement.",
            f"monte_carlo_linked={monte_carlo_linked}.",
        ]

        limitations: list[str] = []
        if required_missing:
            limitations.append(
                "Full capital-survivability model unavailable because required inputs are missing: "
                + ", ".join(required_missing)
                + "."
            )
        if not monte_carlo_linked:
            limitations.append("No Monte Carlo simulation paths available for survivability stress estimation.")
        limitations.extend(
            [
                "No regime-aware sequencing, execution shock integration, or portfolio correlation effects are modeled.",
                "Dynamic sizing and stop-policy adaptation are not modeled in this baseline ruin implementation.",
            ]
        )

        baseline_summary_metrics = {
            "probability_of_ruin": float(mc_probability_of_ruin) if mc_probability_of_ruin is not None else None,
            "expected_stress_drawdown": expected_stress_drawdown,
            "survival_probability": (
                float(1.0 - float(mc_probability_of_ruin)) if mc_probability_of_ruin is not None else None
            ),
            "max_tolerable_risk_per_trade": None,
            "minimum_survivable_capital": None,
            "max_consecutive_losses": int(streak_stats["max_consecutive_losses"]),
            "max_consecutive_wins": int(streak_stats["max_consecutive_wins"]),
            "average_losing_streak": float(streak_stats["average_losing_streak"]),
            "median_losing_streak": float(streak_stats["median_losing_streak"]),
            "average_winning_streak": float(streak_stats["average_winning_streak"]),
            "median_winning_streak": float(streak_stats["median_winning_streak"]),
            "longest_losing_streak_pnl": float(streak_stats["longest_losing_streak_pnl"]),
            "longest_losing_streak_r": None,
            "largest_single_loss": float(streak_stats["largest_single_loss"]),
            "largest_single_loss_r": None,
            "worst_drawdown_pct": float(historical_drawdown_stats["worst_drawdown_pct"]),
            "worst_drawdown_duration_trades": int(historical_drawdown_stats["worst_drawdown_duration_trades"]),
            "recovery_trades_after_worst_drawdown": historical_drawdown_stats["recovery_trades_after_worst_drawdown"],
            "risk_amount_per_trade": None,
        }

        baseline_figures: list[dict[str, Any]] = [
            {
                "id": "loss_streak_distribution",
                "type": "bar_groups",
                "title": "Loss Streak Distribution",
                "x_label": "consecutive_losses",
                "y_label": "occurrences",
                "groups": [
                    {
                        "label": str(row["streak_length"]),
                        "value": int(row["count"]),
                    }
                    for row in streak_stats["loss_streak_distribution"]
                ],
            }
        ]
        if levels:
            baseline_figures.insert(
                0,
                {
                    "id": "ruin_probability_curve",
                    "type": "line_series",
                    "title": "Ruin Probability by Drawdown Threshold",
                    "x_label": "drawdown_threshold_pct",
                    "y_label": "probability_of_breach",
                    "x": [float(level.replace("dd_", "")) / 100.0 for level in levels.keys()],
                    "series": [{"name": "ruin_probability", "values": [float(v) for v in levels.values()]}],
                },
            )

        if required_missing:
            limited_summary_metrics = {
                **baseline_summary_metrics,
                "probability_of_ruin": None,
                "survival_probability": None,
            }
            return {
                "status": "limited",
                "summary_metrics": limited_summary_metrics,
                "streak_statistics": streak_stats,
                "drawdown_statistics": historical_drawdown_stats,
                "figures": baseline_figures,
                "interpretation": {
                    "summary": "Ruin analysis is partially constrained, but trade-sequence survivability diagnostics are available.",
                    "positives": (
                        [
                            (
                                f"Observed max losing streak is {int(streak_stats['max_consecutive_losses'])} trades "
                                f"with cumulative PnL {float(streak_stats['longest_losing_streak_pnl']):.2f}."
                            )
                        ]
                        + (
                            ["Monte Carlo stress drawdown context is available."]
                            if monte_carlo_linked and expected_stress_drawdown is not None
                            else []
                        )
                        if pnl.size > 0
                        else []
                    ),
                    "cautions": [
                        "Capital translation of streak burden requires explicit account_size and risk_per_trade_pct."
                    ],
                },
                "warnings": [],
                "assumptions": assumptions,
                "limitations": limitations,
                "recommendations": [
                    "Set explicit account_size and risk_per_trade_pct to activate full capital-survivability translation.",
                    "Validate sizing with Monte Carlo-linked survivability checks before relying on the result.",
                ],
                "metadata": {
                    "ruin_model_type": "fixed_fractional_bootstrap_ruin_model",
                    "stress_method": "iid_bootstrap_trade_pnl",
                    "result_mode": "limited",
                    "model_completeness": {
                        "has_trade_distribution": bool(pnl.size > 0),
                        "has_account_size": explicit_account_size is not None,
                        "has_risk_per_trade_pct": risk_per_trade_pct is not None,
                        "monte_carlo_linked": monte_carlo_linked,
                    },
                    "required_inputs": ["trades", "account_size", "risk_per_trade_pct"],
                    "missing_required_inputs": required_missing,
                    "ruin_threshold_fraction": ruin_threshold_fraction,
                    "ruin_threshold_equity": ruin_threshold_equity if monte_carlo_linked else None,
                    "capability_used": bool(semantic_capabilities.get("can_build_ruin_model", False)),
                },
                "probability_of_ruin": None,
                "expected_stress_drawdown": expected_stress_drawdown,
                "account_size": float(explicit_account_size) if explicit_account_size is not None else None,
                "risk_per_trade_pct": risk_per_trade_pct,
                "projected_risk_capital_per_trade": None,
            }

        risk_capital_per_trade = float(account_size) * float(risk_per_trade_pct)
        if risk_capital_per_trade <= 0 or pnl.size == 0 or simulations <= 0:
            return {
                "status": "unavailable",
                "summary_metrics": {**baseline_summary_metrics, "probability_of_ruin": None, "survival_probability": None},
                "streak_statistics": streak_stats,
                "drawdown_statistics": historical_drawdown_stats,
                "figures": baseline_figures,
                "interpretation": {
                    "summary": "Ruin analysis unavailable due to invalid sizing capital or missing simulation paths.",
                    "positives": [],
                    "cautions": ["No valid survivability estimate can be produced under current inputs."],
                },
                "warnings": [],
                "assumptions": assumptions,
                "limitations": limitations,
                "recommendations": [
                    "Provide positive account_size and risk_per_trade_pct with simulations > 0 to enable ruin diagnostics."
                ],
                "metadata": {
                    "ruin_model_type": "fixed_fractional_bootstrap_ruin_model",
                    "stress_method": "iid_bootstrap_trade_pnl",
                    "result_mode": "unavailable",
                    "required_inputs": ["trades", "account_size", "risk_per_trade_pct"],
                    "missing_required_inputs": [],
                    "ruin_threshold_fraction": ruin_threshold_fraction,
                    "ruin_threshold_equity": ruin_threshold_equity,
                    "capability_used": bool(semantic_capabilities.get("can_build_ruin_model", False)),
                },
                "probability_of_ruin": None,
                "expected_stress_drawdown": None,
                "account_size": float(account_size),
                "risk_per_trade_pct": float(risk_per_trade_pct),
                "projected_risk_capital_per_trade": risk_capital_per_trade,
            }

        rng_seed = int(monte_carlo.get("methodology", {}).get("seed", 42))
        r_multiple = pnl / risk_capital_per_trade
        scenario_risks = sorted({0.005, 0.01, 0.02, 0.05, float(risk_per_trade_pct)})
        threshold_levels = [0.20, 0.30, 0.40, 0.50, 0.60]

        def _simulate_scenario(risk_pct: float, seed_offset: int) -> tuple[float, float, dict[str, float]]:
            scenario_trade_risk = float(account_size) * float(risk_pct)
            scenario_pnl = r_multiple * scenario_trade_risk
            rng = np.random.default_rng(rng_seed + seed_offset)
            sampled = rng.choice(scenario_pnl, size=(simulations, scenario_pnl.size), replace=True)
            equity_paths = float(account_size) + np.cumsum(sampled, axis=1)
            running_peaks = np.maximum.accumulate(equity_paths, axis=1)
            drawdowns = np.where(running_peaks > 0, (equity_paths - running_peaks) / running_peaks, 0.0)
            max_drawdowns = drawdowns.min(axis=1)
            p_ruin = float((equity_paths.min(axis=1) <= ruin_threshold_equity).mean())
            expected_drawdown = float(max_drawdowns.mean() * 100.0)
            by_threshold = {
                f"dd_{int(level * 100)}": float((max_drawdowns <= -level).mean())
                for level in threshold_levels
            }
            return p_ruin, expected_drawdown, by_threshold

        risk_scenarios: list[dict[str, float]] = []
        active_threshold_curve: dict[str, float] = {}
        probability_of_ruin = None
        for idx, scenario_risk in enumerate(scenario_risks):
            p_ruin, scenario_expected_drawdown, threshold_curve = _simulate_scenario(scenario_risk, idx + 1)
            risk_scenarios.append(
                {
                    "risk_per_trade_pct": float(scenario_risk),
                    "probability_of_ruin": p_ruin,
                    "expected_stress_drawdown": scenario_expected_drawdown,
                }
            )
            if abs(scenario_risk - float(risk_per_trade_pct)) < 1e-9:
                probability_of_ruin = p_ruin
                expected_stress_drawdown = scenario_expected_drawdown
                active_threshold_curve = threshold_curve

        survival_probability = (1.0 - probability_of_ruin) if probability_of_ruin is not None else None
        tolerable = [
            row["risk_per_trade_pct"]
            for row in risk_scenarios
            if row["probability_of_ruin"] <= 0.05
        ]
        max_tolerable_risk = max(tolerable) if tolerable else None
        risk_amount_per_trade = float(risk_capital_per_trade)
        longest_losing_streak_r = (
            float(streak_stats["longest_losing_streak_pnl"]) / risk_amount_per_trade if risk_amount_per_trade > 0 else None
        )
        largest_single_loss_r = (
            float(streak_stats["largest_single_loss"]) / risk_amount_per_trade if risk_amount_per_trade > 0 else None
        )
        estimated_capital_hit_worst_historical_losing_streak = abs(float(streak_stats["longest_losing_streak_pnl"]))
        estimated_capital_hit_p95_drawdown = (
            abs(float(p95_drawdown_pct)) * float(account_size) / 100.0 if p95_drawdown_pct is not None else None
        )
        estimated_capital_hit_worst_simulated_drawdown = (
            abs(float(worst_simulated_drawdown_pct)) * float(account_size) / 100.0
            if worst_simulated_drawdown_pct is not None
            else None
        )
        remaining_capital_after_worst_historical_streak = float(account_size) - estimated_capital_hit_worst_historical_losing_streak
        remaining_capital_after_p95_drawdown = (
            float(account_size) - float(estimated_capital_hit_p95_drawdown)
            if estimated_capital_hit_p95_drawdown is not None
            else None
        )
        remaining_capital_after_worst_simulated_drawdown = (
            float(account_size) - float(estimated_capital_hit_worst_simulated_drawdown)
            if estimated_capital_hit_worst_simulated_drawdown is not None
            else None
        )
        baseline_vs_stressed_scenarios = []
        for row in risk_scenarios:
            risk_label = "baseline"
            if row["risk_per_trade_pct"] < float(risk_per_trade_pct):
                risk_label = "de_risked"
            elif row["risk_per_trade_pct"] > float(risk_per_trade_pct):
                risk_label = "stressed"
            baseline_vs_stressed_scenarios.append(
                {
                    "scenario": risk_label,
                    "risk_per_trade_pct": float(row["risk_per_trade_pct"]),
                    "probability_of_ruin": float(row["probability_of_ruin"]),
                    "expected_stress_drawdown": float(row["expected_stress_drawdown"]),
                }
            )

        figures = [
            {
                "id": "ruin_probability_curve",
                "type": "line_series",
                "title": "Ruin Probability by Drawdown Threshold",
                "x_label": "drawdown_threshold_pct",
                "y_label": "probability_of_breach",
                "x": [float(level.replace("dd_", "")) / 100.0 for level in active_threshold_curve.keys()],
                "series": [{"name": "ruin_probability", "values": [float(v) for v in active_threshold_curve.values()]}],
            },
            {
                "id": "risk_per_trade_sensitivity",
                "type": "line_series",
                "title": "Ruin Sensitivity by Risk per Trade",
                "x_label": "risk_per_trade_pct",
                "y_label": "probability_of_ruin",
                "x": [float(row["risk_per_trade_pct"]) for row in risk_scenarios],
                "series": [{"name": "probability_of_ruin", "values": [float(row["probability_of_ruin"]) for row in risk_scenarios]}],
            },
            {
                "id": "loss_streak_distribution",
                "type": "bar_groups",
                "title": "Loss Streak Distribution",
                "x_label": "consecutive_losses",
                "y_label": "occurrences",
                "groups": [
                    {"label": str(row["streak_length"]), "value": int(row["count"])}
                    for row in streak_stats["loss_streak_distribution"]
                ],
            },
        ]

        recommendations = [
            "Use explicit account_size and risk_per_trade_pct as required controls for survivability decisions.",
            "Run Monte Carlo-linked ruin checks whenever sizing assumptions change.",
        ]
        if probability_of_ruin is not None and probability_of_ruin > 0.10:
            recommendations.append("Current sizing fails the validation threshold; reduce risk_per_trade_pct or increase capital before relying on it.")
        elif probability_of_ruin is not None and probability_of_ruin > 0.03:
            recommendations.append("Sizing appears aggressive; consider reducing risk_per_trade_pct and retesting.")

        cautions = []
        if probability_of_ruin is not None:
            cautions.append(f"Estimated ruin probability is {probability_of_ruin:.2%} at current sizing assumptions.")
        if expected_stress_drawdown is not None:
            cautions.append(f"Expected stress drawdown is {expected_stress_drawdown:.2f}% across simulated paths.")
        if estimated_capital_hit_worst_historical_losing_streak > 0:
            cautions.append(
                "Worst observed losing streak implies an estimated capital hit of "
                f"{estimated_capital_hit_worst_historical_losing_streak:.2f} at current sizing."
            )
        if max_tolerable_risk is not None and float(risk_per_trade_pct) > max_tolerable_risk:
            cautions.append(
                f"Current risk_per_trade_pct ({float(risk_per_trade_pct):.2%}) is above the model's "
                f"max tolerable level ({float(max_tolerable_risk):.2%}) for <=5% ruin."
            )

        sizing_demand = "moderate"
        if longest_losing_streak_r is not None and longest_losing_streak_r <= -8.0:
            sizing_demand = "high"
        elif longest_losing_streak_r is not None and longest_losing_streak_r <= -4.0:
            sizing_demand = "elevated"
        reduction_hint = max(float(risk_per_trade_pct) * 0.5, 0.0025)

        return {
            "status": "available",
            "summary_metrics": {
                "probability_of_ruin": probability_of_ruin,
                "expected_stress_drawdown": expected_stress_drawdown,
                "survival_probability": survival_probability,
                "max_tolerable_risk_per_trade": max_tolerable_risk,
                "minimum_survivable_capital": (
                    float(abs(streak_stats["longest_losing_streak_pnl"]) / float(risk_per_trade_pct))
                    if float(risk_per_trade_pct) > 0
                    else None
                ),
                "max_consecutive_losses": int(streak_stats["max_consecutive_losses"]),
                "max_consecutive_wins": int(streak_stats["max_consecutive_wins"]),
                "average_losing_streak": float(streak_stats["average_losing_streak"]),
                "median_losing_streak": float(streak_stats["median_losing_streak"]),
                "average_winning_streak": float(streak_stats["average_winning_streak"]),
                "median_winning_streak": float(streak_stats["median_winning_streak"]),
                "longest_losing_streak_pnl": float(streak_stats["longest_losing_streak_pnl"]),
                "longest_losing_streak_r": float(longest_losing_streak_r) if longest_losing_streak_r is not None else None,
                "risk_amount_per_trade": risk_amount_per_trade,
                "largest_single_loss": float(streak_stats["largest_single_loss"]),
                "largest_single_loss_r": float(largest_single_loss_r) if largest_single_loss_r is not None else None,
                "worst_drawdown_pct": float(historical_drawdown_stats["worst_drawdown_pct"]),
                "worst_drawdown_duration_trades": int(historical_drawdown_stats["worst_drawdown_duration_trades"]),
                "recovery_trades_after_worst_drawdown": historical_drawdown_stats["recovery_trades_after_worst_drawdown"],
            },
            "figures": figures,
            "risk_scenarios": risk_scenarios,
            "baseline_vs_stressed_scenarios": baseline_vs_stressed_scenarios,
            "streak_statistics": {
                **streak_stats,
                "longest_losing_streak_r": float(longest_losing_streak_r) if longest_losing_streak_r is not None else None,
                "largest_single_loss_r": float(largest_single_loss_r) if largest_single_loss_r is not None else None,
            },
            "drawdown_statistics": historical_drawdown_stats,
            "capital_survivability": {
                "risk_amount_per_trade": risk_amount_per_trade,
                "estimated_capital_hit_worst_historical_losing_streak": estimated_capital_hit_worst_historical_losing_streak,
                "estimated_capital_hit_p95_drawdown": estimated_capital_hit_p95_drawdown,
                "estimated_capital_hit_worst_simulated_drawdown": estimated_capital_hit_worst_simulated_drawdown,
                "remaining_capital_after_worst_historical_streak": remaining_capital_after_worst_historical_streak,
                "remaining_capital_after_p95_drawdown": remaining_capital_after_p95_drawdown,
                "remaining_capital_after_worst_simulated_drawdown": remaining_capital_after_worst_simulated_drawdown,
            },
            "interpretation": {
                "summary": "Ruin estimate is active with explicit capital and risk-per-trade assumptions, including streak burden translation.",
                "positives": (
                    [f"Survival probability is {survival_probability:.2%} under current assumptions."]
                    if survival_probability is not None
                    else []
                ),
                "cautions": cautions,
                "sizing_posture": (
                    f"Current sizing appears {sizing_demand}; longest losing streak equates to "
                    f"{longest_losing_streak_r:.2f}R and "
                    f"{estimated_capital_hit_worst_historical_losing_streak / float(account_size):.2%} of capital."
                    if longest_losing_streak_r is not None and float(account_size) > 0
                    else "Sizing posture estimate unavailable."
                ),
                "sizing_improvement_hint": (
                    "Reducing risk_per_trade_pct to "
                    f"{reduction_hint:.2%} would linearly reduce per-streak capital damage and lower ruin odds."
                ),
            },
            "warnings": [],
            "assumptions": assumptions,
            "limitations": limitations,
            "recommendations": recommendations,
            "metadata": {
                "ruin_model_type": "fixed_fractional_bootstrap_ruin_model",
                "stress_method": "iid_bootstrap_trade_pnl",
                "result_mode": "direct_monte_carlo_linked",
                "scenario_count": len(risk_scenarios),
                "model_completeness": {
                    "has_trade_distribution": True,
                    "has_account_size": True,
                    "has_risk_per_trade_pct": True,
                    "monte_carlo_linked": True,
                },
                "required_inputs": ["trades", "account_size", "risk_per_trade_pct"],
                "missing_required_inputs": [],
                "ruin_threshold_fraction": ruin_threshold_fraction,
                "ruin_threshold_equity": ruin_threshold_equity,
                "capability_used": bool(semantic_capabilities.get("can_build_ruin_model", True)),
                "sizing_model": "fixed_fractional",
                "compounding_model": "fixed_notional_over_horizon",
                "iid_trade_sequencing_assumed": True,
                "non_advisory_language": True,
                "monte_carlo_linked": True,
                "drawdown_levels": list(levels.keys()),
            },
            "probability_of_ruin": probability_of_ruin,
            "probability_drawdown_30": float(active_threshold_curve.get("dd_30", 0.0)),
            "probability_drawdown_50": float(active_threshold_curve.get("dd_50", 0.0)),
            "expected_stress_drawdown": expected_stress_drawdown,
            "capital_threshold": ruin_threshold_equity,
            "risk_amount_per_trade": risk_amount_per_trade,
            "max_consecutive_losses": int(streak_stats["max_consecutive_losses"]),
            "max_consecutive_wins": int(streak_stats["max_consecutive_wins"]),
            "average_losing_streak": float(streak_stats["average_losing_streak"]),
            "median_losing_streak": float(streak_stats["median_losing_streak"]),
            "average_winning_streak": float(streak_stats["average_winning_streak"]),
            "median_winning_streak": float(streak_stats["median_winning_streak"]),
            "longest_losing_streak_pnl": float(streak_stats["longest_losing_streak_pnl"]),
            "longest_losing_streak_r": float(longest_losing_streak_r) if longest_losing_streak_r is not None else None,
            "largest_single_loss": float(streak_stats["largest_single_loss"]),
            "largest_single_loss_r": float(largest_single_loss_r) if largest_single_loss_r is not None else None,
            "worst_drawdown_pct": float(historical_drawdown_stats["worst_drawdown_pct"]),
            "worst_drawdown_duration_trades": int(historical_drawdown_stats["worst_drawdown_duration_trades"]),
            "recovery_trades_after_worst_drawdown": historical_drawdown_stats["recovery_trades_after_worst_drawdown"],
            "estimated_capital_hit_worst_historical_losing_streak": estimated_capital_hit_worst_historical_losing_streak,
            "estimated_capital_hit_p95_drawdown": estimated_capital_hit_p95_drawdown,
            "estimated_capital_hit_worst_simulated_drawdown": estimated_capital_hit_worst_simulated_drawdown,
            "remaining_capital_after_worst_historical_streak": remaining_capital_after_worst_historical_streak,
            "remaining_capital_after_p95_drawdown": remaining_capital_after_p95_drawdown,
            "remaining_capital_after_worst_simulated_drawdown": remaining_capital_after_worst_simulated_drawdown,
            "probability_by_drawdown_threshold": {
                key: float(value) for key, value in active_threshold_curve.items()
            },
            "median_simulated_drawdown_pct": float(median_simulated_drawdown_pct) if median_simulated_drawdown_pct is not None else None,
            "p95_simulated_drawdown_pct": float(p95_drawdown_pct) if p95_drawdown_pct is not None else None,
            "worst_simulated_drawdown_pct": (
                float(worst_simulated_drawdown_pct) if worst_simulated_drawdown_pct is not None else None
            ),
            "account_size": float(account_size),
            "risk_per_trade_pct": float(risk_per_trade_pct),
            "projected_risk_capital_per_trade": risk_capital_per_trade,
        }

    def _normalize_prop_evaluation_rules(
        self,
        raw_rules: dict[str, Any] | None,
        *,
        account_size: float | None,
    ) -> dict[str, Any]:
        source = str((raw_rules or {}).get("source") or "fallback")
        label = str((raw_rules or {}).get("label") or (raw_rules or {}).get("firm_label") or "Fallback evaluation profile")

        def number(name: str, default: float) -> float:
            value = (raw_rules or {}).get(name)
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                parsed = default
            return parsed if np.isfinite(parsed) else default

        def pct(name: str, default: float) -> float:
            value = number(name, default)
            return value / 100.0 if abs(value) > 1.0 else value

        normalized = {
            "schema_version": "prop_evaluation_rules_v1",
            "source": source,
            "label": label,
            "firm_label": str((raw_rules or {}).get("firm_label") or label),
            "account_size": number("account_size", float(account_size or 100_000.0)),
            "profit_target_pct": pct("profit_target_pct", 0.10),
            "max_total_drawdown_pct": pct("max_total_drawdown_pct", 0.10),
            "total_drawdown_basis": str((raw_rules or {}).get("total_drawdown_basis") or "static"),
            "max_daily_loss_pct": pct("max_daily_loss_pct", 0.05),
            "daily_loss_basis": str((raw_rules or {}).get("daily_loss_basis") or "closed_balance"),
            "reset_timezone": str((raw_rules or {}).get("reset_timezone") or "UTC"),
            "minimum_trading_days": int(number("minimum_trading_days", 0.0)),
            "maximum_evaluation_days": int(number("maximum_evaluation_days", 0.0)),
            "consistency_max_day_profit_pct": pct("consistency_max_day_profit_pct", 0.0),
            "max_leverage": number("max_leverage", 0.0),
        }
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        normalized["rules_hash"] = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        normalized["fallback"] = source == "fallback"
        return normalized

    def _prop_evaluation_readiness(
        self,
        *,
        run: IngestedRun,
        rules: dict[str, Any],
        config: AnalysisRunConfig,
    ) -> dict[str, Any]:
        trades = run.trades.copy()
        account_size = float(rules.get("account_size") or config.account_size or 100_000.0)
        profit_target = account_size * float(rules.get("profit_target_pct") or 0.10)
        max_total_drawdown = account_size * float(rules.get("max_total_drawdown_pct") or 0.10)
        max_daily_loss = account_size * float(rules.get("max_daily_loss_pct") or 0.05)
        fallback = bool(rules.get("fallback", False))

        if trades.empty or "pnl_net" not in trades.columns:
            return {
                "status": "unavailable",
                "available": False,
                "limited": True,
                "verdict": "unknown",
                "summary_metrics": {},
                "rule_snapshot": rules,
                "rule_status": [],
                "limitations": ["Prop Evaluation Readiness requires trade PnL or an equity path."],
                "recommendations": ["Upload trade PnL, account sizing, and exact firm rules."],
                "assumptions": ["Fallback prop evaluation rules are preview assumptions unless user-entered rules were supplied."],
                "figures": [],
                "interpretation": {"summary": "Prop evaluation readiness is unavailable without path data."},
                "metadata": {"result_mode": "unavailable", "rules_hash": rules.get("rules_hash")},
            }

        pnl = trades["pnl_net"].fillna(0.0).to_numpy(dtype=float)
        equity = account_size + np.cumsum(pnl)
        dates = self._prop_trade_dates(trades, fallback_count=len(pnl))
        daily_pnl: dict[str, float] = {}
        trading_days: list[str] = []
        for date, value in zip(dates, pnl.tolist(), strict=False):
            if date not in daily_pnl:
                trading_days.append(date)
            daily_pnl[date] = daily_pnl.get(date, 0.0) + float(value)

        first_breach: dict[str, Any] | None = None
        peak = account_size
        max_total_drawdown_observed = 0.0
        for idx, current_equity in enumerate(equity.tolist()):
            peak = max(peak, float(current_equity))
            basis = str(rules.get("total_drawdown_basis") or "static")
            reference = peak if "trailing" in basis else account_size
            drawdown = max(0.0, reference - float(current_equity))
            max_total_drawdown_observed = max(max_total_drawdown_observed, drawdown)
            if first_breach is None and drawdown > max_total_drawdown:
                first_breach = {
                    "type": "max_total_drawdown",
                    "index": idx + 1,
                    "date": dates[idx],
                    "observed": drawdown,
                    "allowed": max_total_drawdown,
                    "margin": drawdown - max_total_drawdown,
                }

        max_daily_loss_observed = max((abs(min(0.0, value)) for value in daily_pnl.values()), default=0.0)
        if first_breach is None:
            for day in trading_days:
                loss = abs(min(0.0, daily_pnl[day]))
                if loss > max_daily_loss:
                    first_breach = {
                        "type": "max_daily_loss",
                        "date": day,
                        "observed": loss,
                        "allowed": max_daily_loss,
                        "margin": loss - max_daily_loss,
                    }
                    break

        final_profit = float(equity[-1] - account_size) if len(equity) else 0.0
        hit_index = next((idx for idx, value in enumerate(equity.tolist()) if value - account_size >= profit_target), None)
        target_hit = hit_index is not None
        target_hit_date = dates[hit_index] if hit_index is not None else None
        consistency_limit = float(rules.get("consistency_max_day_profit_pct") or 0.0)
        consistency_allowed = profit_target * consistency_limit if consistency_limit > 0 else None
        largest_day_profit = max((max(0.0, value) for value in daily_pnl.values()), default=0.0)
        consistency_breach = consistency_allowed is not None and largest_day_profit > consistency_allowed
        if first_breach is None and consistency_breach:
            first_breach = {
                "type": "consistency_rule",
                "observed": largest_day_profit,
                "allowed": consistency_allowed,
                "margin": largest_day_profit - float(consistency_allowed),
            }

        min_days = int(rules.get("minimum_trading_days") or 0)
        max_days = int(rules.get("maximum_evaluation_days") or 0)
        trading_day_count = len(trading_days)
        min_days_met = trading_day_count >= min_days if min_days > 0 else True
        max_days_met = trading_day_count <= max_days if max_days > 0 else True

        passed = bool(target_hit and first_breach is None and min_days_met and max_days_met)
        verdict = "pass_ready" if passed else ("breach_risk" if first_breach else "target_not_reached")
        status = "limited" if fallback or not min_days_met or not max_days_met else "available"
        target_probability, breach_probability = self._prop_bootstrap_probabilities(
            pnl=pnl,
            account_size=account_size,
            profit_target=profit_target,
            max_total_drawdown=max_total_drawdown,
            max_daily_loss=max_daily_loss,
            seed=config.seed,
            simulations=min(max(config.simulations, 0), 1000),
        )

        rule_status = [
            self._prop_rule_status("profit_target", target_hit, final_profit, profit_target),
            self._prop_rule_status("max_total_drawdown", max_total_drawdown_observed <= max_total_drawdown, max_total_drawdown_observed, max_total_drawdown),
            self._prop_rule_status("max_daily_loss", max_daily_loss_observed <= max_daily_loss, max_daily_loss_observed, max_daily_loss),
            self._prop_rule_status("minimum_trading_days", min_days_met, trading_day_count, min_days if min_days > 0 else None),
            self._prop_rule_status("maximum_evaluation_days", max_days_met, trading_day_count, max_days if max_days > 0 else None),
        ]
        if consistency_allowed is not None:
            rule_status.append(self._prop_rule_status("consistency_rule", not consistency_breach, largest_day_profit, consistency_allowed))

        limitations = []
        if fallback:
            limitations.append("Fallback prop evaluation rules were used; replace them with the actual firm rules before relying on this report.")
        if not bool(run.metadata.get("equity_curve_provenance") == "engine_emitted"):
            limitations.append("Daily loss is estimated from closed trade PnL; intraday equity/open PnL was not supplied.")
        recommendations = [
            "Enter the exact prop firm rules and recompute readiness.",
            "Reduce risk per trade or add a daily stop below the firm limit if breach risk is high.",
            "Upload broker/equity path evidence to strengthen daily-loss conclusions.",
        ]
        summary = (
            "Prop evaluation rules were passed on the supplied path."
            if passed
            else f"Prop evaluation readiness is {verdict.replace('_', ' ')} under the selected rules."
        )
        return {
            "status": status,
            "available": True,
            "limited": status == "limited",
            "verdict": verdict,
            "rule_snapshot": rules,
            "rule_status": rule_status,
            "first_breach": first_breach,
            "target_progress": {
                "profit_target": profit_target,
                "final_profit": final_profit,
                "target_hit": target_hit,
                "target_hit_date": target_hit_date,
                "target_progress_pct": (final_profit / profit_target * 100.0) if profit_target else None,
            },
            "summary_metrics": {
                "target_before_breach_probability": target_probability,
                "breach_probability": breach_probability,
                "max_daily_loss_observed": max_daily_loss_observed,
                "max_daily_loss_allowed": max_daily_loss,
                "max_total_drawdown_observed": max_total_drawdown_observed,
                "max_total_drawdown_allowed": max_total_drawdown,
                "trading_days": trading_day_count,
            },
            "figures": [],
            "interpretation": {
                "summary": summary,
                "positives": ["Profit target was reached before a breach."] if passed else [],
                "cautions": limitations,
            },
            "warnings": limitations,
            "assumptions": [
                f"Account size: {account_size:.2f}.",
                f"Profit target: {float(rules.get('profit_target_pct') or 0.0):.2%}.",
                f"Maximum daily loss: {float(rules.get('max_daily_loss_pct') or 0.0):.2%}.",
                f"Maximum total drawdown: {float(rules.get('max_total_drawdown_pct') or 0.0):.2%}.",
            ],
            "limitations": limitations,
            "recommendations": recommendations,
            "metadata": {
                "result_mode": "fallback_preview" if fallback else "user_rules",
                "rules_hash": rules.get("rules_hash"),
                "non_advisory": True,
                "target_hit_index": hit_index,
            },
        }

    def _prop_trade_dates(self, trades: pd.DataFrame, *, fallback_count: int) -> list[str]:
        source = trades.get("exit_ts", trades.get("entry_ts"))
        if source is None:
            return [f"trade_day_{idx + 1}" for idx in range(fallback_count)]
        dates: list[str] = []
        for idx, value in enumerate(source.tolist()):
            try:
                parsed = pd.to_datetime(value, utc=True)
                dates.append(parsed.strftime("%Y-%m-%d"))
            except Exception:  # noqa: BLE001
                dates.append(f"trade_day_{idx + 1}")
        return dates

    def _prop_rule_status(self, rule: str, passed: bool, observed: float | int, allowed: float | int | None) -> dict[str, Any]:
        status = "pass" if passed else "fail"
        if allowed is None:
            status = "not_configured"
        return {"rule": rule, "status": status, "observed": observed, "allowed": allowed}

    def _prop_bootstrap_probabilities(
        self,
        *,
        pnl: np.ndarray,
        account_size: float,
        profit_target: float,
        max_total_drawdown: float,
        max_daily_loss: float,
        seed: int,
        simulations: int,
    ) -> tuple[float | None, float | None]:
        if pnl.size == 0 or simulations <= 0:
            return None, None
        rng = np.random.default_rng(seed + 917)
        target_hits = 0
        breaches = 0
        for _ in range(simulations):
            sampled = rng.choice(pnl, size=pnl.size, replace=True)
            equity = account_size
            peak = account_size
            hit = False
            breach = False
            for value in sampled.tolist():
                equity += float(value)
                peak = max(peak, equity)
                if float(value) < -max_daily_loss or account_size - equity > max_total_drawdown or peak - equity > max_total_drawdown:
                    breach = True
                    break
                if equity - account_size >= profit_target:
                    hit = True
                    break
            target_hits += int(hit and not breach)
            breaches += int(breach)
        return float(target_hits / simulations), float(breaches / simulations)

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
        available_diagnostics = {
            "overview": True,
            "distribution": True,
            "monte_carlo": bool(monte_carlo.get("simulations", 0) > 0),
            "stability": parameter_stability.get("status") != "single_run_only",
            "execution": True,
            "regimes": True,
            "ruin": True,
            "report": True,
        }
        robustness_score = float(score.get("overall", 0.0))
        trade_count = int(run.performance.get("total_trades", 0))
        win_rate = run.performance.get("win_rate")
        expectancy = run.performance.get("ev_net")
        has_trade_only_source = run.source == "trade_log"
        richer_context_signals = (
            bool(run.metadata.get("benchmark_present"))
            or bool(run.metadata.get("ohlcv_present"))
            or bool(run.metadata.get("assumptions_present"))
            or bool(run.metadata.get("params_present"))
            or run.source in {"run_artifacts", "parsed_artifact"}
        )

        executive_verdict = self._report_executive_verdict(
            robustness_score=robustness_score,
            trade_count=trade_count,
            parameter_stability=parameter_stability,
            monte_carlo=monte_carlo,
            risk_of_ruin=risk_of_ruin,
            richer_context_signals=richer_context_signals,
        )
        confidence_level = self._report_confidence_level(
            run=run,
            available_diagnostics=available_diagnostics,
            parameter_stability=parameter_stability,
            risk_of_ruin=risk_of_ruin,
            simulations=simulations,
        )
        diagnostics_summary = self._report_diagnostics_summary(
            available_diagnostics=available_diagnostics,
            robustness_score=robustness_score,
            monte_carlo=monte_carlo,
            parameter_stability=parameter_stability,
            execution_sensitivity=execution_sensitivity,
            regime=regime,
            risk_of_ruin=risk_of_ruin,
        )

        limitations = [
            "Diagnostic confidence is bounded by uploaded artifact richness and may improve with benchmark/OHLCV/parameter-grid context.",
        ]
        if has_trade_only_source:
            limitations.append(
                "Trade-only uploads cannot fully validate parameter topology or true market-regime robustness."
            )
        if parameter_stability.get("status") == "single_run_only":
            limitations.append(
                "Parameter stability currently relies on a single-run proxy rather than grid-topology evidence."
            )
        if risk_of_ruin.get("status") != "supported":
            limitations.append(
                "Risk-of-ruin estimates are limited without explicit account_size and risk_per_trade_pct assumptions."
            )
        if simulations <= 0:
            limitations.append("Monte Carlo simulations were disabled, reducing survivability evidence depth.")

        deployment_guidance = self._report_deployment_guidance(
            verdict_status=executive_verdict["status"],
            confidence_level=confidence_level["level"],
            risk_of_ruin=risk_of_ruin,
            parameter_stability=parameter_stability,
            has_trade_only_source=has_trade_only_source,
            trade_count=trade_count,
        )
        recommendations = self._report_recommendations(
            parameter_stability=parameter_stability,
            risk_of_ruin=risk_of_ruin,
            has_trade_only_source=has_trade_only_source,
            diagnostics_summary=diagnostics_summary,
        )

        analysis_date = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        strategy_name = run.metadata.get("strategy_name")

        canonical_report = {
            "executive_verdict": executive_verdict,
            "confidence_level": confidence_level,
            "executive_summary": {
                "summary": (
                    f"Analyzed {trade_count} realized trades for strategy '{strategy_name or 'unknown_strategy'}' "
                    f"with deterministic diagnostics; robustness score is {robustness_score:.1f}/100, "
                    f"with win rate {float(win_rate or 0.0):.1%} and expectancy {float(expectancy or 0.0):.4f}."
                ),
                "operational_implications": deployment_guidance["narrative"],
                "what_matters_now": recommendations[:3],
            },
            "diagnostics_summary": diagnostics_summary,
            "methodology": {
                "engine": "StrategyRobustnessLabService",
                "runtime_seam": "run_analysis_from_parsed_artifact / build_dashboard_payload",
                "artifact_richness": run.metadata.get("richness", "trade_only"),
                "ingestion_source": run.source,
                "modeling_assumptions": [
                    "Deterministic metric synthesis from uploaded artifacts.",
                    "Monte Carlo uses IID bootstrap sequencing of realized trade outcomes.",
                    "Execution stress scenarios perturb fees/slippage/spread from baseline assumptions.",
                ],
                "monte_carlo": {
                    "enabled": bool(simulations > 0),
                    "seed": int(seed),
                    "simulations": int(simulations),
                    "ruin_drawdown_levels": list(monte_carlo.get("metadata", {}).get("drawdown_levels", [])),
                },
                "parser_notes": list(run.metadata.get("parser_notes", [])),
            },
            "limitations": limitations,
            "deployment_guidance": deployment_guidance,
            "recommendations": recommendations,
            "key_metrics_snapshot": {
                "robustness_score": robustness_score,
                "win_rate": float(win_rate) if win_rate is not None else None,
                "expectancy": float(expectancy) if expectancy is not None else None,
                "worst_simulated_drawdown_pct": float(monte_carlo.get("worst_drawdown_pct", 0.0)),
                "probability_of_ruin": risk_of_ruin.get("probability_of_ruin"),
                "edge_decay_pct": execution_sensitivity.get("summary_metrics", {}).get("edge_decay_pct"),
            },
            "report_figures": self._report_curated_figures(
                risk_of_ruin=risk_of_ruin,
            ),
            "metadata": {
                "report_scope": "validation_report",
                "artifact_label": strategy_name,
                "analysis_date": analysis_date,
                "analysis_id": f"{strategy_name or 'strategy'}::{analysis_date}",
                "available_diagnostics": available_diagnostics,
                "proof_report_contract_version": STRATEGY_TRUTH_ROOM_CONTRACT_VERSION,
                "export_readiness": {
                    "screen_rendering_ready": True,
                    "pdf_ready_core_sections": True,
                    "audit_share_structured": True,
                },
            },
        }
        return {
            "report": canonical_report,
            "summary_metrics": {
                "robustness_score": robustness_score,
                "trade_count": trade_count,
                "available_diagnostic_count": int(sum(1 for enabled in available_diagnostics.values() if enabled)),
            },
            "figures": [],
            "interpretation": {
                "summary": f"Final posture: {executive_verdict['headline']}.",
                "positives": [],
                "cautions": ["Report summarizes only diagnostics that were truthfully computable from supplied inputs."],
            },
            "warnings": [],
            "assumptions": [
                "Report sections are synthesized from deterministic diagnostics using configured Monte Carlo seed."
            ],
            "recommendations": recommendations,
            "metadata": {
                "available_diagnostics": available_diagnostics,
                "proof_report_contract_version": STRATEGY_TRUTH_ROOM_CONTRACT_VERSION,
                "required_proof_sections": [
                    "executive_verdict",
                    "artifact_identity",
                    "evidence_coverage",
                    "assumption_ledger",
                    "unsupported_claims",
                    "limitations",
                    "next_evidence",
                ],
                "export_sections": [
                    "executive_summary",
                    "diagnostics_summary",
                    "methodology",
                    "limitations",
                    "deployment_guidance",
                    "recommendations",
                ],
                "compatibility": {
                    "deprecated_aliases_removed": [
                        "executive_summary",
                        "validation_posture",
                        "diagnostics_summary",
                        "methodology",
                        "deployment_guidance",
                        "confidence_level",
                        "executive_verdict",
                        "key_metrics_snapshot",
                        "report_figures",
                        "strategy_summary",
                        "assumptions_detail",
                        "performance_summary",
                        "monte_carlo_diagnostics",
                        "parameter_stability",
                        "execution_sensitivity",
                        "regime_analysis",
                        "risk_of_ruin",
                        "score",
                        "final_verdict",
                    ],
                    "canonical_report_path": "diagnostics.report.report",
                },
            },
        }

    def _report_executive_verdict(
        self,
        *,
        robustness_score: float,
        trade_count: int,
        parameter_stability: dict[str, Any],
        monte_carlo: dict[str, Any],
        risk_of_ruin: dict[str, Any],
        richer_context_signals: bool,
    ) -> dict[str, str]:
        mc_drawdown = float(monte_carlo.get("worst_drawdown_pct", 0.0))
        ruin_probability = risk_of_ruin.get("probability_of_ruin")
        stability_proxy_only = parameter_stability.get("status") == "single_run_only"
        thin_evidence = trade_count < 30 or not richer_context_signals

        status = "robust"
        if robustness_score < 45.0 or mc_drawdown <= -45.0:
            status = "not_deployment_ready"
        elif robustness_score < 60.0 or stability_proxy_only:
            status = "fragile"
        elif thin_evidence or (ruin_probability is not None and float(ruin_probability) > 0.15):
            status = "conditional"

        headline_map = {
            "robust": "Robust under evaluated assumptions",
            "conditional": "Conditionally deployable with controls",
            "fragile": "Fragile under current assumptions",
            "not_deployment_ready": "Not deployment-ready",
        }
        return {
            "status": status,
            "headline": headline_map[status],
            "summary": (
                f"Verdict reflects robustness score {robustness_score:.1f}, worst simulated drawdown {mc_drawdown:.1f}% "
                f"and evidence depth from {trade_count} trades."
            ),
        }

    def _report_confidence_level(
        self,
        *,
        run: IngestedRun,
        available_diagnostics: dict[str, bool],
        parameter_stability: dict[str, Any],
        risk_of_ruin: dict[str, Any],
        simulations: int,
    ) -> dict[str, str]:
        score = 0
        if run.performance.get("total_trades", 0) >= 100:
            score += 2
        elif run.performance.get("total_trades", 0) >= 30:
            score += 1
        if run.source in {"run_artifacts", "parsed_artifact"}:
            score += 1
        if parameter_stability.get("status") != "single_run_only":
            score += 1
        if risk_of_ruin.get("status") == "supported":
            score += 1
        if simulations >= 500:
            score += 1
        if all(available_diagnostics.values()):
            score += 1

        if score >= 6:
            level = "high"
        elif score >= 3:
            level = "medium"
        else:
            level = "low"

        return {
            "level": level,
            "summary": (
                f"Confidence is {level} based on trade sample size, diagnostic completeness, "
                "and whether sizing-sensitive ruin analysis was fully supported."
            ),
        }

    def _report_diagnostics_summary(
        self,
        *,
        available_diagnostics: dict[str, bool],
        robustness_score: float,
        monte_carlo: dict[str, Any],
        parameter_stability: dict[str, Any],
        execution_sensitivity: dict[str, Any],
        regime: dict[str, Any],
        risk_of_ruin: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        execution_score = float(execution_sensitivity.get("execution_resilience_score", 0.0))
        return {
            "overview": {
                "status": "available" if available_diagnostics["overview"] else "unavailable",
                "takeaway": f"Topline robustness score is {robustness_score:.1f}/100.",
                "confidence_impact": "supports" if robustness_score >= 60.0 else "weakens",
            },
            "distribution": {
                "status": "available" if available_diagnostics["distribution"] else "unavailable",
                "takeaway": "Trade return distribution has been profiled for shape and tail behavior.",
                "confidence_impact": "supports",
            },
            "monte_carlo": {
                "status": "available" if available_diagnostics["monte_carlo"] else "unavailable",
                "takeaway": f"Worst simulated drawdown is {float(monte_carlo.get('worst_drawdown_pct', 0.0)):.1f}%.",
                "confidence_impact": (
                    "supports" if float(monte_carlo.get("worst_drawdown_pct", 0.0)) > -35.0 else "weakens"
                ),
            },
            "stability": {
                "status": "available" if available_diagnostics["stability"] else "limited",
                "takeaway": (
                    "Stability informed by parameter grid context."
                    if parameter_stability.get("status") != "single_run_only"
                    else "Stability currently proxy-only because parameter sweep context is missing."
                ),
                "confidence_impact": (
                    "supports" if parameter_stability.get("status") != "single_run_only" else "weakens"
                ),
            },
            "execution": {
                "status": "available" if available_diagnostics["execution"] else "unavailable",
                "takeaway": f"Execution resilience score is {execution_score:.1f}/100 under stress scenarios.",
                "confidence_impact": "supports" if execution_score >= 60.0 else "weakens",
            },
            "regimes": {
                "status": "available" if available_diagnostics["regimes"] else "unavailable",
                "takeaway": (
                    "Regime diagnostics support consistent performance across proxy buckets."
                    if regime.get("status") == "supported"
                    else "Regime diagnostics are proxy-limited and should be validated with OHLCV context."
                ),
                "confidence_impact": "supports" if regime.get("status") == "supported" else "weakens",
            },
            "ruin": {
                "status": risk_of_ruin.get("status", "limited"),
                "takeaway": (
                    "Sizing-aware ruin probability is available."
                    if risk_of_ruin.get("status") == "supported"
                    else "Ruin model is limited because explicit sizing assumptions are incomplete."
                ),
                "confidence_impact": "supports" if risk_of_ruin.get("status") == "supported" else "weakens",
            },
            "report": {
                "status": "available",
                "takeaway": "Validation report synthesizes diagnostic evidence into deployment framing.",
                "confidence_impact": "supports",
            },
        }

    def _report_deployment_guidance(
        self,
        *,
        verdict_status: str,
        confidence_level: str,
        risk_of_ruin: dict[str, Any],
        parameter_stability: dict[str, Any],
        has_trade_only_source: bool,
        trade_count: int,
    ) -> dict[str, Any]:
        deploy_now = verdict_status == "robust" and confidence_level in {"high", "medium"}
        return {
            "deploy_now": deploy_now,
            "recommended_scope": (
                "Pilot capital with explicit risk caps and monitoring."
                if deploy_now
                else "Research/sandbox only until critical evidence gaps are closed."
            ),
            "do_not_use_for": [
                "Full-scale capital deployment without sizing-aware ruin validation.",
                "Unsupervised production rollout without execution drift monitoring.",
            ],
            "required_conditions_before_deploy": [
                "Provide explicit account_size and risk_per_trade_pct for ruin calibration."
                if risk_of_ruin.get("status") != "supported"
                else "Maintain ruin guardrails and enforce sizing limits.",
                "Validate parameter stability with sweep/grid evidence."
                if parameter_stability.get("status") == "single_run_only"
                else "Re-check parameter stability after each major strategy change.",
                "Expand artifact bundle with OHLCV + benchmark for broader robustness context."
                if has_trade_only_source or trade_count < 30
                else "Maintain periodic out-of-sample and regime validation cadence.",
            ],
            "narrative": (
                "Current evidence supports cautious pilot usage only."
                if deploy_now
                else "Current evidence does not support live deployment yet."
            ),
        }

    def _report_recommendations(
        self,
        *,
        parameter_stability: dict[str, Any],
        risk_of_ruin: dict[str, Any],
        has_trade_only_source: bool,
        diagnostics_summary: dict[str, dict[str, Any]],
    ) -> list[str]:
        recommendations: list[str] = []
        if has_trade_only_source:
            recommendations.append(
                "Upload richer artifact bundles (OHLCV, benchmark, parameter grid) to unlock full-fidelity diagnostics."
            )
        if parameter_stability.get("status") == "single_run_only":
            recommendations.append(
                "Run a parameter sweep and confirm plateau stability before committing capital."
            )
        if risk_of_ruin.get("status") != "supported":
            recommendations.append(
                "Specify account_size and risk_per_trade_pct, then rerun ruin diagnostics before deployment."
            )
        if diagnostics_summary.get("execution", {}).get("confidence_impact") == "weakens":
            recommendations.append("Reduce sizing and tighten cost assumptions until execution resilience improves.")
        recommendations.append("Re-validate out-of-sample performance before any production promotion decision.")
        return recommendations

    def _report_curated_figures(
        self,
        *,
        risk_of_ruin: dict[str, Any],
    ) -> list[dict[str, str]]:
        figures = [
            {
                "section": "overview",
                "figure_key": "equity_curve",
                "title": "Equity curve overview",
            },
            {
                "section": "distribution",
                "figure_key": "return_histogram",
                "title": "Trade return distribution histogram",
            },
            {
                "section": "monte_carlo",
                "figure_key": "fan_chart_paths",
                "title": "Monte Carlo equity fan",
            },
        ]
        if risk_of_ruin.get("status") == "supported":
            figures.append(
                {
                    "section": "ruin",
                    "figure_key": "ruin_probability_curve",
                    "title": "Ruin probability by drawdown threshold",
                }
            )
        return figures


def run_analysis_from_parsed_artifact(
    parsed_artifact: ParsedArtifactInput,
    *,
    config: AnalysisRunConfig | None = None,
) -> EngineAnalysisResult:
    """Central engine seam for normalized parsed-artifact analysis."""
    service = StrategyRobustnessLabService()
    return service.run_analysis_from_parsed_artifact(parsed_artifact, config=config)
