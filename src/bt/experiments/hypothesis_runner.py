"""Standardized hypothesis-contract runner."""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml

from bt.api import run_backtest
from bt.analytics.segment_rollups import build_run_segment_rollups
from bt.analytics.l7_h1_evaluation import write_l7_h1_evaluation_artifacts, write_signal_feature_artifact
from bt.config import load_yaml
from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import MissingRequiredTierError
from bt.hypotheses.logging import make_log_row
from bt.logging.artifacts_manifest import write_artifacts_manifest
from bt.logging.run_contract import validate_run_artifacts
from bt.logging.run_manifest import write_run_manifest
from bt.logging.summary import write_summary_txt
from bt.metrics.per_symbol import write_per_symbol_metrics
from bt.analysis.ev_by_bucket import run_structural_bucket_analysis


DEFAULT_VOLATILE_RESEARCH_PANEL_CHUNKSIZE = 50_000


def resolve_phase_tiers(contract: HypothesisContract, phase: str) -> tuple[str, ...]:
    required = contract.required_tiers()
    if phase == "tier2":
        return tuple(t for t in required if t == "Tier2")
    if phase == "tier3":
        return tuple(t for t in required if t == "Tier3")
    if phase == "validate":
        return required
    raise ValueError("phase must be one of: tier2, tier3, validate")


def validation_status(contract: HypothesisContract, observed_tiers: set[str]) -> str:
    missing = [tier for tier in contract.required_tiers() if tier not in observed_tiers]
    return "validated" if not missing else "incomplete"


def run_hypothesis_contract(
    contract: HypothesisContract,
    *,
    executor: Callable[[dict[str, Any], str], dict[str, Any]],
    symbol: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
    available_tiers: set[str],
    execution_model_name: str = "engine_default",
    phase: str = "validate",
) -> list[dict[str, Any]]:
    tiers_to_run = resolve_phase_tiers(contract, phase)
    if not tiers_to_run:
        raise MissingRequiredTierError(f"phase '{phase}' did not resolve to any required tiers")
    missing = [tier for tier in tiers_to_run if tier not in available_tiers]
    if missing:
        raise MissingRequiredTierError(f"missing required tiers for phase '{phase}': {missing}")

    rows: list[dict[str, Any]] = []
    for spec in contract.to_run_specs():
        for tier in tiers_to_run:
            result = executor(spec, tier)
            base = {
                "run_id": f"{spec['hypothesis_id']}::{spec['grid_id']}::{tier}",
                "hypothesis_id": spec["hypothesis_id"],
                "title": spec["title"],
                "contract_version": spec["contract_version"],
                "grid_id": spec["grid_id"],
                "config_hash": spec["config_hash"],
                "symbol": symbol,
                "timeframe": timeframe,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "tier": tier,
                "execution_model_name": execution_model_name,
                "params_json": spec["params"],
                "indicators_json": list(contract.required_indicators()),
                "gates_json": list(contract.schema.gates),
                "validation_status": validation_status(contract, {tier}),
            }
            rows.append(make_log_row(base, result))
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run hypothesis contract variants on the production backtest engine")
    parser.add_argument("--config", required=True, help="Base engine config YAML path")
    parser.add_argument("--local-config", help="Optional local override YAML")
    parser.add_argument("--data", required=True, help="Canonical 1m data path (dataset dir or file)")
    parser.add_argument("--out", required=True, help="Output root directory")
    parser.add_argument("--hypothesis", required=True, help="Hypothesis YAML path")
    parser.add_argument("--phase", choices=("tier2", "tier3", "validate"), default="validate")
    parser.add_argument("--override", action="append", default=[], help="Additional override YAML paths")
    return parser


def _tier_to_execution_profile(tier: str) -> str:
    mapping = {"Tier2": "tier2", "Tier3": "tier3", "Tier1": "tier1"}
    return mapping.get(tier, "tier2")


def build_runtime_override(contract: HypothesisContract, spec: dict[str, Any], tier: str) -> dict[str, Any]:
    entry = contract.schema.entry
    spec_params = spec.get("params", {})
    signal_timeframe = str(
        spec_params.get(
            "signal_timeframe",
            spec_params.get("timeframe", entry.get("signal_timeframe", entry.get("timeframe", "15m"))),
        )
    ).lower()
    sem = contract.schema.execution_semantics
    htf_timeframes = [signal_timeframe]
    if isinstance(sem, dict) and str(sem.get("strategy_family", "")).lower() == "regime_switch":
        branch_high = sem.get("branch_high_vol") if isinstance(sem.get("branch_high_vol"), dict) else {}
        branch_low = sem.get("branch_low_vol") if isinstance(sem.get("branch_low_vol"), dict) else {}
        high_tf = str(branch_high.get("signal_timeframe", "15m")).lower()
        low_tf = str(branch_low.get("signal_timeframe", "5m")).lower()
        htf_timeframes = sorted(set([high_tf, low_tf]))
    if sem:
        expected_base = str(sem.get("base_data_frequency_expected", "1m")).lower()
        exit_tf = str(sem.get("exit_monitoring_timeframe", "1m")).lower()
        if expected_base != "1m" or exit_tf != "1m":
            raise ValueError("L1-H1 runner requires canonical 1m base data and 1m exit monitoring.")

    data_override: dict[str, Any] = {
        "engine_timeframe": None,
        "entry_timeframe": None,
        "exit_timeframe": "1m",
    }
    strategy_name = str(entry.get("strategy", "l1_h1_vol_floor_trend"))
    use_compiled_features = spec_params.get("use_compiled_features")
    if use_compiled_features is None:
        use_compiled_features = True
    if strategy_name == "l7_h1_csi_gated_displacement_trend" and bool(use_compiled_features):
        from bt.engine.fast_path.l7_h1_kernel import prefix_for_timeframe

        data_override["extra_column_prefixes"] = [prefix_for_timeframe(signal_timeframe)]
        data_override["requires_htf_context"] = False
    elif bool(use_compiled_features):
        data_override["extra_column_prefixes"] = [f"htf_{tf}_" for tf in htf_timeframes]
        data_override["htf_context_source"] = "precomputed"

    strategy_params = dict(spec["params"])
    if strategy_name != "l7_h1_csi_gated_displacement_trend":
        # These are runner fast-path hints, not public constructor parameters
        # for ordinary strategies. The generic stable fast path swaps only the
        # HTF context source; strategy logic remains byte-for-byte classic.
        strategy_params.pop("use_compiled_features", None)
        strategy_params.pop("use_compiled_event_kernel", None)
        strategy_params.pop("compiled_event_source", None)

    strategy_payload = {
        "name": strategy_name,
        "signal_conflict_policy": "reject",
        **strategy_params,
        "timeframe": signal_timeframe,
        "disallow_flip": bool(entry.get("disallow_flip", True)),
    }
    if strategy_name == "l7_h1_csi_gated_displacement_trend":
        strategy_payload["use_compiled_event_kernel"] = bool(use_compiled_features)

    return {
        "identity": {
            "hypothesis_id": str(spec["hypothesis_id"]),
            "grid_id": str(spec["grid_id"]),
            "tier": tier,
            "strategy_id": strategy_name,
        },
        "data": data_override,
        "execution": {
            "profile": _tier_to_execution_profile(tier),
        },
        "htf_resampler": {
            "timeframes": htf_timeframes,
            "strict": True,
        },
        "strategy": strategy_payload,
        "indicator_profile": "none" if strategy_name == "l7_h1_csi_gated_displacement_trend" and bool(use_compiled_features) else "default",
        "state_features": {
            "enabled": False,
            "profile": "full",
        } if strategy_name == "l7_h1_csi_gated_displacement_trend" and bool(use_compiled_features) else {},
    }


def _research_panel_universe_from_overrides(override_paths: list[str] | None) -> str | None:
    for raw_path in override_paths or []:
        path = Path(raw_path)
        if not path.exists():
            continue
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        data = payload.get("data")
        if not isinstance(data, dict):
            continue
        if data.get("dataset_kind") != "research_panel":
            continue
        universe = data.get("universe")
        return str(universe) if universe is not None else None
    return None


def _volatile_research_panel_chunksize() -> int:
    raw = os.environ.get("BT_RESEARCH_VOLATILE_CHUNKSIZE")
    if raw is None:
        return DEFAULT_VOLATILE_RESEARCH_PANEL_CHUNKSIZE
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("BT_RESEARCH_VOLATILE_CHUNKSIZE must be an integer") from exc
    if value <= 0:
        raise ValueError("BT_RESEARCH_VOLATILE_CHUNKSIZE must be positive")
    return value


def apply_runtime_data_memory_controls(runtime_override: dict[str, Any], override_paths: list[str] | None) -> None:
    """Apply late data-loader controls that are safe and semantics-neutral.

    Volatile research-panel streaming keeps one paused iterator per historical
    membership symbol. A large parquet batch size causes every paused iterator
    to retain a large DataFrame, which can multiply into tens of GB per worker.
    Reducing the batch size changes only IO granularity; it does not alter bar
    order, membership gating, fills, or no-lookahead behavior.
    """
    universe = _research_panel_universe_from_overrides(override_paths)
    strategy = runtime_override.get("strategy") if isinstance(runtime_override.get("strategy"), dict) else {}
    data = runtime_override.get("data") if isinstance(runtime_override.get("data"), dict) else {}
    if (
        universe == "volatile"
        and isinstance(strategy, dict)
        and strategy.get("name") == "l7_h1_csi_gated_displacement_trend"
    ):
        wants_event_kernel = bool(strategy.get("use_compiled_event_kernel"))
        if os.environ.get("BT_ALLOW_VOLATILE_L7H1_COMPILED", "").strip() == "1" and wants_event_kernel:
            # Volatile membership streams are path-dependent: active rows come
            # from the materialized feed, while continuation rows for live
            # positions/orders come from individual symbol panels. Static
            # precomputed columns cannot exactly match that stream. The online
            # event source keeps the fast engine controls while computing the
            # L7-H1 feature state from exactly the emitted bars.
            strategy["compiled_event_source"] = "online"
            runtime_override["state_features"] = {"enabled": True, "profile": "full"}
            if isinstance(data, dict):
                data.pop("extra_column_prefixes", None)
                data.pop("extra_columns", None)
                data["requires_htf_context"] = False
        elif wants_event_kernel or bool(strategy.get("use_compiled_features")):
            # Volatile materialization only contains active membership rows.
            # The engine may still request inactive symbols after entry so
            # positions can exit and mark-to-market honestly. Default to the
            # classic strategy feature path unless the online event source is
            # explicitly enabled for comparison/validation.
            strategy["use_compiled_features"] = False
            strategy["use_compiled_event_kernel"] = False
            runtime_override["indicator_profile"] = "default"
            runtime_override["state_features"] = {"enabled": True, "profile": "full"}
            if isinstance(data, dict):
                data.pop("extra_column_prefixes", None)
                data.pop("extra_columns", None)
                data.pop("requires_htf_context", None)
        else:
            strategy.pop("compiled_event_source", None)
            if isinstance(data, dict):
                data.pop("requires_htf_context", None)
    if universe != "volatile":
        return
    data = runtime_override.setdefault("data", {})
    if not isinstance(data, dict):
        raise ValueError("runtime override data section must be a mapping")
    data["chunksize"] = _volatile_research_panel_chunksize()




def _postprocess_run_artifacts(run_dir: Path, *, data_path: str) -> None:
    validate_run_artifacts(run_dir)
    write_per_symbol_metrics(run_dir)
    write_signal_feature_artifact(run_dir)

    config_path = run_dir / "config_used.yaml"
    try:
        loaded_config = load_yaml(config_path)
    except Exception as exc:  # pragma: no cover - defensive user-facing guard
        raise ValueError(f"Unable to read config_used.yaml from run_dir={run_dir}: {exc}") from exc
    if not isinstance(loaded_config, dict):
        raise ValueError(f"Invalid config_used.yaml format in run_dir={run_dir}; expected mapping.")

    config: dict[str, Any] = loaded_config
    hypothesis_id = None
    identity = config.get("identity") if isinstance(config.get("identity"), dict) else {}
    if isinstance(identity.get("hypothesis_id"), str):
        hypothesis_id = identity["hypothesis_id"]
    elif isinstance(config.get("strategy"), dict):
        strategy_name = config["strategy"].get("name")
        if isinstance(strategy_name, str):
            hypothesis_id = strategy_name
    try:
        trades_path = run_dir / "trades.csv"
        if trades_path.exists():
            try:
                trades_df = pd.read_csv(trades_path)
            except pd.errors.EmptyDataError:
                trades_df = pd.DataFrame()
            if not trades_df.empty:
                run_structural_bucket_analysis(trades_df, run_dir, min_trades=10)
                if str(hypothesis_id).upper() == "L7-H1":
                    tier = identity.get("tier") if isinstance(identity, dict) else None
                    write_l7_h1_evaluation_artifacts(run_dir, tier=str(tier) if tier is not None else None)
        write_summary_txt(run_dir)
        write_run_manifest(run_dir, config=config, data_path=data_path)
        build_run_segment_rollups(run_dir, hypothesis_id=hypothesis_id)
    finally:
        write_artifacts_manifest(run_dir, config=config)

def _read_run_metrics(run_dir: Path) -> dict[str, Any]:
    performance_path = run_dir / "performance.json"
    payload = json.loads(performance_path.read_text(encoding="utf-8")) if performance_path.exists() else {}
    return {
        "num_trades": payload.get("trades", 0),
        "ev_r_gross": payload.get("expectancy_r", 0.0),
        "ev_r_net": payload.get("expectancy_r", 0.0),
        "pnl_gross": payload.get("pnl_gross", payload.get("net_pnl", 0.0)),
        "pnl_net": payload.get("net_pnl", 0.0),
        "hit_rate": payload.get("win_rate", 0.0),
        "max_drawdown_r": payload.get("max_drawdown", 0.0),
        "mae_mean_r": payload.get("mae_mean_r", 0.0),
        "mfe_mean_r": payload.get("mfe_mean_r", 0.0),
        "avg_hold_bars": payload.get("avg_hold_bars", 0.0),
    }


def execute_hypothesis_variant(
    *,
    contract: HypothesisContract,
    spec: dict[str, Any],
    tier: str,
    config_path: str,
    data_path: str,
    out_root: str,
    local_config: str | None = None,
    override_paths: list[str] | None = None,
    run_slug: str | None = None,
) -> dict[str, Any]:
    runtime_override = build_runtime_override(contract, spec, tier)
    resolved_override_paths = list(override_paths or [])
    if local_config:
        resolved_override_paths.append(local_config)
    apply_runtime_data_memory_controls(runtime_override, resolved_override_paths)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
        yaml.safe_dump(runtime_override, tmp, sort_keys=True)
        runtime_override_path = tmp.name
    resolved_override_paths.append(runtime_override_path)
    resolved_run_name = run_slug or f"{spec['hypothesis_id'].lower()}_{spec['grid_id']}_{tier.lower()}"
    try:
        run_dir = Path(
            run_backtest(
                config_path=config_path,
                data_path=data_path,
                out_dir=str(out_root),
                override_paths=resolved_override_paths,
                run_name=resolved_run_name,
            )
        )
    finally:
        Path(runtime_override_path).unlink(missing_ok=True)

    _postprocess_run_artifacts(run_dir, data_path=data_path)
    metrics = _read_run_metrics(run_dir)
    metrics["run_dir"] = str(run_dir)
    return metrics


def main() -> None:
    args = build_parser().parse_args()
    contract = HypothesisContract.from_yaml(args.hypothesis)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    base_config = load_yaml(args.config)
    data_cfg = base_config.get("data") if isinstance(base_config.get("data"), dict) else {}
    symbols_subset = data_cfg.get("symbols_subset") if isinstance(data_cfg, dict) else None
    symbol = symbols_subset[0] if isinstance(symbols_subset, list) and symbols_subset else "*"
    date_range = data_cfg.get("date_range") if isinstance(data_cfg, dict) else None
    if isinstance(date_range, dict):
        start_ts = str(date_range.get("start", ""))
        end_ts = str(date_range.get("end", ""))
    else:
        start_ts = ""
        end_ts = ""

    rows: list[dict[str, Any]] = []

    def _executor(spec: dict[str, Any], tier: str) -> dict[str, Any]:
        return execute_hypothesis_variant(
            contract=contract,
            spec=spec,
            tier=tier,
            config_path=args.config,
            data_path=args.data,
            out_root=str(out_root),
            local_config=args.local_config,
            override_paths=list(args.override),
        )

    signal_tf = str(contract.schema.entry.get("signal_timeframe", contract.schema.entry.get("timeframe", "15m"))).lower()
    rows = run_hypothesis_contract(
        contract,
        executor=_executor,
        symbol=symbol,
        timeframe=signal_tf,
        start_ts=start_ts,
        end_ts=end_ts,
        available_tiers={"Tier2", "Tier3"},
        phase=args.phase,
    )

    output_rows_path = out_root / "hypothesis_rows.jsonl"
    with output_rows_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
