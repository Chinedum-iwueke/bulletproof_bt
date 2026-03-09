"""Standardized hypothesis-contract runner."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any, Callable

import yaml

from bt.api import run_backtest
from bt.config import load_yaml
from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.exceptions import MissingRequiredTierError
from bt.hypotheses.logging import make_log_row


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


def _build_runtime_override(contract: HypothesisContract, spec: dict[str, Any], tier: str) -> dict[str, Any]:
    entry = contract.schema.entry
    signal_timeframe = str(entry.get("signal_timeframe", entry.get("timeframe", spec["params"].get("timeframe", "15m")))).lower()
    sem = contract.schema.execution_semantics
    if sem:
        expected_base = str(sem.get("base_data_frequency_expected", "1m")).lower()
        exit_tf = str(sem.get("exit_monitoring_timeframe", "1m")).lower()
        if expected_base != "1m" or exit_tf != "1m":
            raise ValueError("L1-H1 runner requires canonical 1m base data and 1m exit monitoring.")

    return {
        "data": {
            "engine_timeframe": None,
            "entry_timeframe": None,
            "exit_timeframe": "1m",
        },
        "execution": {
            "profile": _tier_to_execution_profile(tier),
        },
        "htf_resampler": {
            "timeframes": [signal_timeframe],
            "strict": True,
        },
        "strategy": {
            "name": str(entry.get("strategy", "l1_h1_vol_floor_trend")),
            "signal_conflict_policy": "reject",
            **spec["params"],
            "timeframe": signal_timeframe,
            "disallow_flip": bool(entry.get("disallow_flip", True)),
        },
    }


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
        runtime_override = _build_runtime_override(contract, spec, tier)
        override_paths = list(args.override)
        if args.local_config:
            override_paths.append(args.local_config)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            yaml.safe_dump(runtime_override, tmp, sort_keys=True)
            runtime_override_path = tmp.name
        override_paths.append(runtime_override_path)
        run_name = f"{spec['hypothesis_id'].lower()}_{spec['grid_id']}_{tier.lower()}"
        try:
            run_dir = Path(
                run_backtest(
                    config_path=args.config,
                    data_path=args.data,
                    out_dir=str(out_root),
                    override_paths=override_paths,
                    run_name=run_name,
                )
            )
        finally:
            Path(runtime_override_path).unlink(missing_ok=True)
        metrics = _read_run_metrics(run_dir)
        metrics["run_dir"] = str(run_dir)
        return metrics

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
