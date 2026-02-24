"""Public product API for running backtests and experiment grids."""
from __future__ import annotations

import json
import traceback
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from bt.config import deep_merge, load_yaml, resolve_paths_relative_to
from bt.core.config_resolver import resolve_config
from bt.execution.effective import build_effective_execution_snapshot
from bt.execution.intrabar import parse_intrabar_spec
from bt.logging.sanity import SanityCounters, write_sanity_json
from bt.logging.formatting import write_json_deterministic
from bt.contracts.schema_versions import (
    BENCHMARK_METRICS_SCHEMA_VERSION,
    COMPARISON_SUMMARY_SCHEMA_VERSION,
)
from bt.validation.config_completeness import validate_resolved_config_completeness


def _resolve_timeframe_mode(config: dict[str, Any]) -> tuple[str, str | None, str | None, str]:
    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    if not isinstance(data_cfg, dict):
        raise ValueError("config.data must be a mapping when provided")

    engine_timeframe_raw = data_cfg.get("engine_timeframe")
    entry_timeframe_raw = data_cfg.get("entry_timeframe")
    legacy_timeframe_raw = data_cfg.get("timeframe")

    if engine_timeframe_raw is None and entry_timeframe_raw is None and legacy_timeframe_raw is not None:
        engine_timeframe_raw = legacy_timeframe_raw

    if engine_timeframe_raw is not None and entry_timeframe_raw is not None:
        raise ValueError("Invalid config: choose one mode (set only one of data.engine_timeframe or data.entry_timeframe)")

    from bt.data.resample import normalize_timeframe

    engine_timeframe = None
    entry_timeframe = None
    mode = "default"

    if engine_timeframe_raw is not None:
        engine_timeframe = normalize_timeframe(engine_timeframe_raw, key_path="data.engine_timeframe")
        mode = "engine_timeframe"
    elif entry_timeframe_raw is not None:
        entry_timeframe = normalize_timeframe(entry_timeframe_raw, key_path="data.entry_timeframe")
        mode = "entry_timeframe"

    exit_timeframe_raw = data_cfg.get("exit_timeframe", "1m")
    exit_timeframe = normalize_timeframe(exit_timeframe_raw, key_path="data.exit_timeframe")
    if mode == "entry_timeframe" and exit_timeframe != "1m":
        raise ValueError("data.exit_timeframe not supported yet; exits are evaluated every engine bar")

    return mode, engine_timeframe, entry_timeframe, exit_timeframe



def _build_engine(
    config: dict[str, Any],
    datafeed: Any,
    run_dir: Path,
    sanity_counters: SanityCounters | None = None,
    audit_manager: AuditManager | None = None,
):
    from bt.core.engine import BacktestEngine
    from bt.data.resample import TimeframeResampler
    from bt.data.resampled_feed import EntryTimeframeGate
    from bt.execution.execution_model import ExecutionModel
    from bt.execution.fees import FeeModel
    from bt.execution.profile import resolve_execution_profile
    from bt.execution.slippage import SlippageModel
    from bt.logging.jsonl import JsonlWriter
    from bt.logging.trades import TradesCsvWriter
    from bt.portfolio.portfolio import Portfolio
    from bt.risk.risk_engine import RiskEngine
    from bt.risk.spec import parse_risk_spec
    from bt.strategy import make_strategy
    from bt.strategy.htf_context import (
        HTFContextStrategyAdapter,
        ReadOnlyContextStrategyAdapter,
        SignalConflictPolicyStrategyAdapter,
    )
    from bt.universe.universe import UniverseEngine

    universe = UniverseEngine(
        min_history_bars=int(config.get("min_history_bars", 1)),
        lookback_bars=int(config.get("lookback_bars", 1)),
        min_avg_volume=float(config.get("min_avg_volume", 0.0)),
        lag_bars=int(config.get("lag_bars", 0)),
    )

    strategy_cfg = config.get("strategy") if isinstance(config.get("strategy"), dict) else {}
    strategy_name = strategy_cfg.get("name", "coinflip")
    strategy_kwargs = {k: v for k, v in strategy_cfg.items() if k != "name"}
    if strategy_name == "volfloor_donchian":
        if "entry_lookback" in strategy_kwargs and "donchian_entry_lookback" not in strategy_kwargs:
            strategy_kwargs["donchian_entry_lookback"] = strategy_kwargs.pop("entry_lookback")
        if "exit_lookback" in strategy_kwargs and "donchian_exit_lookback" not in strategy_kwargs:
            strategy_kwargs["donchian_exit_lookback"] = strategy_kwargs.pop("exit_lookback")
        if "vol_window_days" in strategy_kwargs and "vol_lookback_bars" not in strategy_kwargs:
            strategy_kwargs["vol_lookback_bars"] = int(float(strategy_kwargs.pop("vol_window_days")) * 24 * 4)
    strategy = make_strategy(
        strategy_name,
        seed=int(strategy_kwargs.pop("seed", config.get("seed", 42))),
        **strategy_kwargs,
    )
    strategy = ReadOnlyContextStrategyAdapter(inner=strategy)

    mode, engine_timeframe, entry_timeframe, _ = _resolve_timeframe_mode(config)

    data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
    timeframe_override = data_cfg.get("timeframe") if isinstance(data_cfg, dict) else None
    if timeframe_override is not None and mode == "default":
        from bt.data.resample import normalize_timeframe

        parsed_timeframe = normalize_timeframe(timeframe_override, key_path="data.timeframe")
        raw_htf_resampler = config.get("htf_resampler")
        if isinstance(raw_htf_resampler, dict):
            htf_cfg = dict(raw_htf_resampler)
            htf_cfg["timeframes"] = [parsed_timeframe]
            config["htf_resampler"] = htf_cfg
        else:
            config["htf_resampler"] = {"timeframes": [parsed_timeframe], "strict": True}

    htf_resampler = config.get("htf_resampler")
    if isinstance(htf_resampler, dict):
        htf_resampler = TimeframeResampler(
            timeframes=[str(tf) for tf in htf_resampler.get("timeframes", [])],
            strict=bool(htf_resampler.get("strict", True)),
        )
    if isinstance(htf_resampler, TimeframeResampler):
        strategy = HTFContextStrategyAdapter(inner=strategy, resampler=htf_resampler)

    signal_conflict_policy = strategy_cfg.get("signal_conflict_policy", "reject")
    strategy = SignalConflictPolicyStrategyAdapter(inner=strategy, policy=str(signal_conflict_policy))

    if entry_timeframe is not None:
        strategy = EntryTimeframeGate(inner=strategy, entry_timeframe=entry_timeframe)

    if not isinstance(config.get("risk"), dict):
        raise ValueError("risk.mode and risk.r_per_trade are required")
    risk_cfg = config["risk"]
    risk_cfg_for_spec = dict(risk_cfg)
    if "mode" not in risk_cfg_for_spec:
        risk_cfg_for_spec["mode"] = "equity_pct"
    if "r_per_trade" not in risk_cfg_for_spec:
        if "risk_per_trade_pct" in risk_cfg_for_spec:
            risk_cfg_for_spec["r_per_trade"] = risk_cfg_for_spec["risk_per_trade_pct"]
        elif "risk_per_trade_pct" in config:
            risk_cfg_for_spec["r_per_trade"] = config["risk_per_trade_pct"]
    risk_spec = parse_risk_spec({"risk": risk_cfg_for_spec})

    execution_profile = resolve_execution_profile(config)
    effective_slippage_bps = execution_profile.slippage_bps + execution_profile.spread_bps

    risk = RiskEngine(
        max_positions=int(risk_cfg.get("max_positions", 5)),
        max_notional_per_symbol=config.get("max_notional_per_symbol"),
        margin_buffer_tier=int(risk_cfg.get("margin_buffer_tier", 1)),
        maker_fee_bps=execution_profile.maker_fee * 1e4,
        taker_fee_bps=execution_profile.taker_fee * 1e4,
        slippage_k_proxy=float(risk_cfg.get("slippage_k_proxy", 0.0)),
        config={
            "risk": risk_cfg_for_spec,
            "model": "fixed_bps",
            "fixed_bps": effective_slippage_bps,
            "slippage": config.get("slippage"),
        },
    )

    fee_model = FeeModel(
        maker_fee_bps=execution_profile.maker_fee * 1e4,
        taker_fee_bps=execution_profile.taker_fee * 1e4,
    )
    slippage_model = SlippageModel(
        k=float(config.get("slippage_k", 1.0)),
        atr_pct_cap=float(config.get("atr_pct_cap", 0.20)),
        impact_cap=float(config.get("impact_cap", 0.05)),
        fixed_bps=effective_slippage_bps,
    )
    execution_cfg = config.get("execution") if isinstance(config.get("execution"), dict) else {}
    raw_spread_mode = execution_cfg.get("spread_mode", "none")
    spread_mode = raw_spread_mode if isinstance(raw_spread_mode, str) and raw_spread_mode else "none"

    raw_spread_bps = execution_cfg.get("spread_bps")
    if spread_mode == "fixed_bps" and raw_spread_bps is None:
        spread_bps = execution_profile.spread_bps
    else:
        spread_bps = 0.0 if raw_spread_bps is None else float(raw_spread_bps)

    intrabar_spec = parse_intrabar_spec(config)

    execution = ExecutionModel(
        fee_model=fee_model,
        slippage_model=slippage_model,
        spread_mode=spread_mode,
        spread_bps=spread_bps,
        intrabar_mode=intrabar_spec.mode,
        delay_bars=execution_profile.delay_bars,
    )

    portfolio_max_leverage = risk_spec.max_leverage
    if portfolio_max_leverage is None:
        portfolio_max_leverage = float(config.get("max_leverage", 2.0))

    portfolio = Portfolio(
        initial_cash=float(config.get("initial_cash", 100000.0)),
        max_leverage=portfolio_max_leverage,
    )

    return BacktestEngine(
        datafeed=datafeed,
        universe=universe,
        strategy=strategy,
        risk=risk,
        execution=execution,
        portfolio=portfolio,
        decisions_writer=JsonlWriter(run_dir / "decisions.jsonl"),
        fills_writer=JsonlWriter(run_dir / "fills.jsonl"),
        trades_writer=TradesCsvWriter(run_dir / "trades.csv"),
        equity_path=run_dir / "equity.csv",
        config=config,
        sanity_counters=sanity_counters,
        audit_manager=audit_manager,
    )


def _read_data_scope_for_sanity(run_dir: Path) -> dict[str, Any] | None:
    data_scope_path = run_dir / "data_scope.json"
    if not data_scope_path.exists():
        return None
    try:
        payload = json.loads(data_scope_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None

    date_range = payload.get("date_range")
    if isinstance(date_range, dict):
        return {
            "data_start_ts": date_range.get("start"),
            "data_end_ts": date_range.get("end"),
        }
    return None


def run_backtest(
    *,
    config_path: str,
    data_path: str,
    out_dir: str,
    override_paths: Optional[list[str]] = None,
    run_name: Optional[str] = None,
) -> str:
    """
    Runs a single backtest and returns the created run directory path.
    """
    from bt.benchmark.compare import compare_strategy_vs_benchmark
    from bt.benchmark.metrics import compute_benchmark_metrics
    from bt.benchmark.spec import parse_benchmark_spec
    from bt.benchmark.tracker import BenchmarkTracker, BenchmarkTrackingFeed, write_benchmark_equity_csv
    from bt.data.dataset import load_dataset_manifest
    from bt.data.load_feed import load_feed
    from bt.experiments.grid_runner import _write_run_status
    from bt.logging.trades import make_run_id, prepare_run_dir, write_config_used, write_data_scope
    from bt.metrics.performance import compute_performance, write_performance_artifacts
    from bt.metrics.reconcile import reconcile_execution_costs
    from bt.execution.profile import resolve_execution_profile

    from bt.audit.audit_manager import AuditManager

    base_config = load_yaml(config_path)
    fees_config = load_yaml("configs/fees.yaml")
    slippage_config = load_yaml("configs/slippage.yaml")
    config = deep_merge(base_config, fees_config)
    config = deep_merge(config, slippage_config)
    for override_path in resolve_paths_relative_to(Path(config_path).parent, override_paths):
        config = deep_merge(config, load_yaml(override_path))
    config = resolve_config(config)
    validate_resolved_config_completeness(config)

    resolved_run_name = run_name or make_run_id()
    run_dir = prepare_run_dir(Path(out_dir), resolved_run_name)
    sanity_counters = SanityCounters(run_id=resolved_run_name)
    audit_manager = AuditManager(run_dir=run_dir, config=config, run_id=resolved_run_name)

    try:
        write_config_used(run_dir, config)
        write_data_scope(
            run_dir,
            config=config,
            dataset_dir=data_path if Path(data_path).is_dir() else None,
        )

        benchmark_spec = parse_benchmark_spec(config)
        benchmark_tracker: BenchmarkTracker | None = None
        if benchmark_spec.enabled:
            benchmark_symbol = benchmark_spec.symbol
            if Path(data_path).is_dir():
                manifest = load_dataset_manifest(data_path, config)
                if benchmark_symbol not in manifest.symbols:
                    raise ValueError(
                        f"benchmark.symbol={benchmark_symbol} not found in dataset scope for dataset_dir={data_path}"
                    )
            benchmark_tracker = BenchmarkTracker(benchmark_spec)

        datafeed = load_feed(data_path, config)
        if audit_manager.enabled and hasattr(datafeed, "_bars"):
            from bt.audit.data_audit import run_data_audit

            data_report = run_data_audit(datafeed._bars)
            audit_manager.write_json("data_audit.json", data_report)

        mode, engine_timeframe, _, _ = _resolve_timeframe_mode(config)
        if mode == "engine_timeframe" and engine_timeframe is not None:
            from bt.data.resampled_feed import ResampledDataFeed

            strict = True
            data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
            if isinstance(data_cfg, dict) and "resample_strict" in data_cfg:
                strict = bool(data_cfg.get("resample_strict"))
            elif isinstance(config.get("htf_resampler"), dict):
                strict = bool(config["htf_resampler"].get("strict", True))
            datafeed = ResampledDataFeed(
                inner_feed=datafeed,
                timeframe=engine_timeframe,
                strict=strict,
                audit_manager=audit_manager,
            )

        benchmark_metrics: dict[str, Any] | None = None
        if benchmark_tracker is not None:
            datafeed = BenchmarkTrackingFeed(inner_feed=datafeed, tracker=benchmark_tracker)

        try:
            engine = _build_engine(
                config,
                datafeed,
                run_dir,
                sanity_counters=sanity_counters,
                audit_manager=audit_manager,
            )
        except TypeError:
            engine = _build_engine(
                config,
                datafeed,
                run_dir,
                sanity_counters=sanity_counters,
            )
        engine.run()

        if benchmark_tracker is not None:
            benchmark_initial_equity = (
                benchmark_spec.initial_equity
                if benchmark_spec.initial_equity is not None
                else float(config.get("initial_cash", 100000.0))
            )
            benchmark_points = benchmark_tracker.finalize(initial_equity=benchmark_initial_equity)
            write_benchmark_equity_csv(benchmark_points, run_dir / "benchmark_equity.csv")
            benchmark_metrics = compute_benchmark_metrics(equity_points=benchmark_points)
            benchmark_metrics["schema_version"] = BENCHMARK_METRICS_SCHEMA_VERSION
            write_json_deterministic(run_dir / "benchmark_metrics.json", benchmark_metrics)

        report = compute_performance(run_dir)
        write_performance_artifacts(report, run_dir)
        reconcile_execution_costs(run_dir)

        if benchmark_spec.enabled:
            if benchmark_metrics is None:
                raise ValueError(
                    f"benchmark enabled but benchmark_metrics.json was not produced for run_dir={run_dir}"
                )
            comparison_summary = compare_strategy_vs_benchmark(
                strategy_perf=asdict(report),
                bench_metrics=benchmark_metrics,
            )
            comparison_summary["schema_version"] = COMPARISON_SUMMARY_SCHEMA_VERSION
            write_json_deterministic(run_dir / "comparison_summary.json", comparison_summary)

        if bool((config.get("audit") or {}).get("determinism_check", False)):
            from bt.audit.determinism import build_output_hashes, compare_hashes
            rerun_dir = prepare_run_dir(Path(out_dir), f"{resolved_run_name}_determinism")
            write_config_used(rerun_dir, config)
            write_data_scope(
                rerun_dir,
                config=config,
                dataset_dir=data_path if Path(data_path).is_dir() else None,
            )
            datafeed2 = load_feed(data_path, config)
            try:
                engine2 = _build_engine(
                    config,
                    datafeed2,
                    rerun_dir,
                    sanity_counters=SanityCounters(run_id=f"{resolved_run_name}_determinism"),
                    audit_manager=None,
                )
            except TypeError:
                engine2 = _build_engine(
                    config,
                    datafeed2,
                    rerun_dir,
                    sanity_counters=SanityCounters(run_id=f"{resolved_run_name}_determinism"),
                )
            engine2.run()
            report_det = compare_hashes(build_output_hashes(run_dir), build_output_hashes(rerun_dir))
            audit_manager.write_json("determinism_report.json", report_det)

        execution_snapshot = build_effective_execution_snapshot(config)
        _write_run_status(
            run_dir,
            {
                "status": "PASS",
                "error_type": "",
                "error_message": "",
                "traceback": "",
                "run_id": resolved_run_name,
                **execution_snapshot,
            },
            config=config,
        )
    except Exception as exc:
        status_payload = {
            "status": "FAIL",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
            "run_id": resolved_run_name,
            "intrabar_mode": parse_intrabar_spec(config).mode,
        }
        try:
            status_payload.update(build_effective_execution_snapshot(config))
        except ValueError:
            pass
        _write_run_status(
            run_dir,
            status_payload,
            config=config,
        )
        raise
    finally:
        write_sanity_json(
            run_dir,
            sanity_counters,
            data_scope=_read_data_scope_for_sanity(run_dir),
        )
        try:
            audit_manager.write_coverage_json()
        except Exception:
            pass

    return str(run_dir)


def run_grid(
    *,
    config_path: str,
    experiment_path: str,
    data_path: str,
    out_dir: str,
    override_paths: Optional[list[str]] = None,
    experiment_name: Optional[str] = None,
) -> str:
    """
    Runs an experiment grid and returns the created experiment directory path.
    """
    from bt.experiments.grid_runner import run_grid as run_grid_library

    base = load_yaml(config_path)
    fees = load_yaml("configs/fees.yaml")
    slippage = load_yaml("configs/slippage.yaml")
    config = deep_merge(base, fees)
    config = deep_merge(config, slippage)

    for override_path in resolve_paths_relative_to(Path(config_path).parent, override_paths):
        config = deep_merge(config, load_yaml(override_path))

    config = resolve_config(config)
    validate_resolved_config_completeness(config)

    experiment_cfg = load_yaml(experiment_path)
    resolved_experiment_name = experiment_name or str(experiment_cfg.get("name") or "experiment")
    experiment_dir = Path(out_dir) / resolved_experiment_name

    run_grid_library(
        config=config,
        experiment_cfg=experiment_cfg,
        data_path=data_path,
        out_path=experiment_dir,
        force=False,
    )

    return str(experiment_dir)
