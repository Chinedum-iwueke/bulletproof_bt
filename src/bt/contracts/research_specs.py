from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import yaml


ALLOWED_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d"}
ALLOWED_DATASETS = {
    "ohlcv",
    "trades",
    "funding",
    "open_interest",
    "mark_price",
    "index_price",
    "liquidations",
    "benchmark",
    "research_panel",
}
ALLOWED_FAMILIES = {
    "trend_continuation",
    "mean_reversion",
    "breakout",
    "volatility_filter",
    "funding_liquidation_context",
}
ALLOWED_EXPERIMENT_TYPES = {
    "baseline",
    "cost_sensitivity",
    "slippage_sensitivity",
    "parameter_grid",
    "holdout_split",
    "benchmark_null",
    "regime_state_split",
    "alternative_exit",
}
REGISTERED_SIGNAL_FUNCTIONS = {
    "ema_cross",
    "donchian_breakout",
    "vwap_reversion",
    "atr_stop",
    "volatility_percentile_gate",
    "funding_extreme_gate",
    "liquidation_impulse_gate",
    "time_stop",
}
FIELD_ALIASES = {
    "close": "ohlcv",
    "open": "ohlcv",
    "high": "ohlcv",
    "low": "ohlcv",
    "volume": "ohlcv",
    "vwap": "ohlcv",
    "atr": "ohlcv",
    "ema": "ohlcv",
    "funding_rate": "funding",
    "open_interest": "open_interest",
    "mark_price": "mark_price",
    "index_price": "index_price",
    "liquidation_notional": "liquidations",
}
LOOKAHEAD_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"lookahead",
        r"future",
        r"lead\s*\(",
        r"shift\s*\(\s*-",
        r"next_bar",
        r"tomorrow",
    )
]
CARD_SCHEMA_VERSION = "strategy_research_terminal.card.v1"
CARD_BUNDLE_SCHEMA_VERSION = "strategy_research_terminal.bundle.v1"
B7_CARD_TYPES = (
    "HypothesisCard",
    "RunQualityCard",
    "ExecutionDragCard",
    "FailureCauseCard",
    "RegimeStateDependencyCard",
    "ParameterFragilityCard",
    "NullComparisonCard",
    "VerdictCard",
    "NextExperimentCard",
)


class SpecValidationError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("\n".join(errors))
        self.errors = errors


def load_spec(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        loaded = yaml.safe_load(text)
    else:
        loaded = json.loads(text)
    if not isinstance(loaded, dict):
        raise SpecValidationError(["spec_root_must_be_object"])
    return loaded


def _missing(doc: dict[str, Any], fields: list[str]) -> list[str]:
    return [field for field in fields if doc.get(field) in (None, "", [], {})]


def validate_hypothesis_spec(spec: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = [
        "schema_version",
        "hypothesis_id",
        "title",
        "thesis",
        "market_mechanism",
        "observable_features",
        "entry_condition_intent",
        "exit_condition_intent",
        "invalidation_criteria",
        "required_datasets",
        "cost_model_assumptions",
        "benchmark_or_null",
        "expected_failure_modes",
        "safe_parameter_ranges",
        "out_of_sample_plan",
        "execution_semantics",
    ]
    errors += [f"missing_{field}" for field in _missing(spec, required)]
    if spec.get("schema_version") != "hypothesis_spec_v1":
        errors.append("schema_version_must_be_hypothesis_spec_v1")
    datasets = spec.get("required_datasets")
    if isinstance(datasets, list):
        unknown = [str(item) for item in datasets if item not in ALLOWED_DATASETS]
        errors += [f"unknown_required_dataset_{item}" for item in unknown]
    else:
        errors.append("required_datasets_must_be_list")
    timeframe = (spec.get("execution_semantics") or {}).get("signal_timeframe")
    if timeframe and timeframe not in ALLOWED_TIMEFRAMES:
        errors.append("execution_semantics_signal_timeframe_unsupported")
    ranges = spec.get("safe_parameter_ranges")
    if isinstance(ranges, dict):
        for name, value in ranges.items():
            if not isinstance(value, dict) or not isinstance(value.get("min"), (int, float)) or not isinstance(value.get("max"), (int, float)):
                errors.append(f"safe_parameter_range_{name}_must_have_numeric_min_max")
            elif value["min"] >= value["max"]:
                errors.append(f"safe_parameter_range_{name}_min_must_be_less_than_max")
    else:
        errors.append("safe_parameter_ranges_must_be_object")
    if not isinstance(spec.get("invalidation_criteria"), list) or len(spec.get("invalidation_criteria") or []) == 0:
        errors.append("invalidation_criteria_must_be_non_empty_list")
    return errors


def explain_missing_hypothesis_spec(spec: dict[str, Any]) -> list[str]:
    errors = validate_hypothesis_spec(spec)
    explanations = {
        "missing_market_mechanism": "State why the edge should exist before asking the engine to test it.",
        "missing_observable_features": "List causal fields the strategy can observe at or before decision time.",
        "missing_invalidation_criteria": "Define what evidence would kill the thesis.",
        "missing_required_datasets": "Declare OHLCV, funding, OI, liquidation, or benchmark inputs before run generation.",
        "missing_safe_parameter_ranges": "Bound every tunable parameter before search.",
        "missing_out_of_sample_plan": "Pre-register holdout or walk-forward validation before optimization.",
    }
    return [explanations.get(error, error) for error in errors]


def validate_strategy_spec(spec: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = [
        "schema_version",
        "strategy_spec_id",
        "hypothesis_id",
        "strategy_family",
        "universe",
        "timeframe",
        "required_datasets",
        "signals",
        "parameters",
        "cost_model",
        "slippage_model",
        "risk_model",
        "execution_semantics",
        "compiler",
    ]
    errors += [f"missing_{field}" for field in _missing(spec, required)]
    if spec.get("schema_version") != "strategy_spec_v1":
        errors.append("schema_version_must_be_strategy_spec_v1")
    if spec.get("strategy_family") not in ALLOWED_FAMILIES:
        errors.append("strategy_family_unsupported")
    if spec.get("timeframe") not in ALLOWED_TIMEFRAMES:
        errors.append("timeframe_unsupported")
    if not isinstance(spec.get("universe"), list) or len(spec.get("universe") or []) == 0:
        errors.append("universe_must_be_non_empty_list")
    datasets = spec.get("required_datasets") if isinstance(spec.get("required_datasets"), list) else []
    if not datasets:
        errors.append("required_datasets_must_be_non_empty_list")
    for dataset in datasets:
        if dataset not in ALLOWED_DATASETS:
            errors.append(f"unknown_required_dataset_{dataset}")
    execution = spec.get("execution_semantics") if isinstance(spec.get("execution_semantics"), dict) else {}
    if execution.get("lookahead_allowed") is not False:
        errors.append("lookahead_allowed_must_be_false")
    if execution.get("interpolation_allowed") is not False:
        errors.append("interpolation_allowed_must_be_false_unless_explicitly_reviewed")
    if execution.get("signal_bar_policy") not in {"closed_bar_only", "base_bar_only"}:
        errors.append("signal_bar_policy_must_be_closed_bar_only_or_base_bar_only")
    for model_name in ("cost_model", "slippage_model", "risk_model"):
        model = spec.get(model_name)
        if not isinstance(model, dict) or not model:
            errors.append(f"{model_name}_must_be_declared_object")
    parameters = spec.get("parameters")
    if isinstance(parameters, dict):
        for name, value in parameters.items():
            if not isinstance(value, dict):
                errors.append(f"parameter_{name}_must_be_object")
                continue
            if "default" not in value:
                errors.append(f"parameter_{name}_missing_default")
            if not isinstance(value.get("min"), (int, float)) or not isinstance(value.get("max"), (int, float)):
                errors.append(f"parameter_{name}_must_have_numeric_min_max")
            elif value["min"] >= value["max"]:
                errors.append(f"parameter_{name}_min_must_be_less_than_max")
    else:
        errors.append("parameters_must_be_object")
    signals = spec.get("signals")
    if not isinstance(signals, list) or not signals:
        errors.append("signals_must_be_non_empty_list")
    else:
        for index, signal in enumerate(signals):
            if not isinstance(signal, dict):
                errors.append(f"signal_{index}_must_be_object")
                continue
            function = signal.get("function")
            if function not in REGISTERED_SIGNAL_FUNCTIONS:
                errors.append(f"signal_{index}_function_not_registered")
            expression = json.dumps(signal, sort_keys=True)
            if any(pattern.search(expression) for pattern in LOOKAHEAD_PATTERNS):
                errors.append(f"signal_{index}_contains_lookahead_language")
            for field in signal.get("fields", []) if isinstance(signal.get("fields"), list) else []:
                dataset = FIELD_ALIASES.get(str(field))
                if dataset and dataset not in datasets and "research_panel" not in datasets:
                    errors.append(f"signal_{index}_field_{field}_requires_dataset_{dataset}")
    return errors


def compile_strategy_run_config(spec: dict[str, Any]) -> dict[str, Any]:
    errors = validate_strategy_spec(spec)
    if errors:
        raise SpecValidationError(errors)
    return {
        "schema_version": "run_config_from_strategy_spec_v1",
        "strategy_spec_id": spec["strategy_spec_id"],
        "hypothesis_id": spec["hypothesis_id"],
        "strategy": {
            "family": spec["strategy_family"],
            "signals": spec["signals"],
            "params": {name: config["default"] for name, config in spec["parameters"].items()},
        },
        "universe": spec["universe"],
        "timeframe": spec["timeframe"],
        "required_datasets": spec["required_datasets"],
        "cost_model": spec["cost_model"],
        "slippage_model": spec["slippage_model"],
        "risk_model": spec["risk_model"],
        "execution_semantics": spec["execution_semantics"],
        "compiler": spec["compiler"],
    }


def build_experiment_plan(strategy_spec: dict[str, Any]) -> dict[str, Any]:
    errors = validate_strategy_spec(strategy_spec)
    if errors:
        raise SpecValidationError(errors)
    datasets = set(strategy_spec.get("required_datasets") or [])
    params = strategy_spec.get("parameters") if isinstance(strategy_spec.get("parameters"), dict) else {}
    cost_bps = float((strategy_spec.get("cost_model") or {}).get("round_trip_bps", 8))
    slip_bps = float((strategy_spec.get("slippage_model") or {}).get("round_trip_bps", 4))
    items: list[dict[str, Any]] = [
        {
            "item_id": "baseline",
            "experiment_type": "baseline",
            "title": "Baseline approved strategy spec",
            "priority": 100,
            "enabled": True,
            "required_datasets": strategy_spec["required_datasets"],
            "runtime_budget": {"max_minutes": 30, "max_variants": 1},
            "config_patch": {},
            "falsification_question": "Does the approved strategy spec survive its declared base assumptions?",
        },
        {
            "item_id": "cost_sensitivity_2x",
            "experiment_type": "cost_sensitivity",
            "title": "Cost sensitivity 2x",
            "priority": 90,
            "enabled": True,
            "required_datasets": strategy_spec["required_datasets"],
            "runtime_budget": {"max_minutes": 30, "max_variants": 1},
            "config_patch": {"cost_model": {**strategy_spec["cost_model"], "round_trip_bps": cost_bps * 2}},
            "falsification_question": "Does the edge survive doubled explicit trading cost?",
        },
        {
            "item_id": "slippage_sensitivity_2x",
            "experiment_type": "slippage_sensitivity",
            "title": "Slippage sensitivity 2x",
            "priority": 85,
            "enabled": True,
            "required_datasets": strategy_spec["required_datasets"],
            "runtime_budget": {"max_minutes": 30, "max_variants": 1},
            "config_patch": {"slippage_model": {**strategy_spec["slippage_model"], "round_trip_bps": slip_bps * 2}},
            "falsification_question": "Does the edge survive worse fills?",
        },
        {
            "item_id": "benchmark_null",
            "experiment_type": "benchmark_null",
            "title": "Benchmark and null comparison",
            "priority": 80,
            "enabled": True,
            "required_datasets": list(sorted(set(strategy_spec["required_datasets"]) | {"benchmark"})),
            "runtime_budget": {"max_minutes": 30, "max_variants": 3},
            "config_patch": {"null_model": "random_entry_same_hold_time"},
            "falsification_question": "Does the strategy beat a simple benchmark and matched random-entry null?",
        },
    ]
    if params:
        items.append(
            {
                "item_id": "parameter_grid_safe",
                "experiment_type": "parameter_grid",
                "title": "Safe parameter grid",
                "priority": 75,
                "enabled": True,
                "required_datasets": strategy_spec["required_datasets"],
                "runtime_budget": {"max_minutes": 90, "max_variants": min(27, max(3, len(params) * 3))},
                "config_patch": {"parameter_grid": params},
                "falsification_question": "Does the result depend on one fragile parameter point?",
            }
        )
    items.append(
        {
            "item_id": "holdout_split_late",
            "experiment_type": "holdout_split",
            "title": "Late-period holdout split",
            "priority": 70,
            "enabled": True,
            "required_datasets": strategy_spec["required_datasets"],
            "runtime_budget": {"max_minutes": 45, "max_variants": 2},
            "config_patch": {"split": {"kind": "time_holdout", "holdout_fraction": 0.33}},
            "falsification_question": "Does the result survive a later out-of-sample window?",
        }
    )
    if {"funding", "open_interest", "liquidations", "research_panel"} & datasets:
        items.append(
            {
                "item_id": "regime_state_split",
                "experiment_type": "regime_state_split",
                "title": "Declared state split",
                "priority": 65,
                "enabled": True,
                "required_datasets": strategy_spec["required_datasets"],
                "runtime_budget": {"max_minutes": 60, "max_variants": 4},
                "config_patch": {"state_split": {"kind": "declared_context_fields"}},
                "falsification_question": "Is performance concentrated in one market state?",
            }
        )
    if any(signal.get("function") == "atr_stop" for signal in strategy_spec.get("signals", []) if isinstance(signal, dict)):
        items.append(
            {
                "item_id": "alternative_exit_wider_stop",
                "experiment_type": "alternative_exit",
                "title": "Alternative exit: wider stop",
                "priority": 60,
                "enabled": True,
                "required_datasets": strategy_spec["required_datasets"],
                "runtime_budget": {"max_minutes": 30, "max_variants": 1},
                "config_patch": {"exit_variant": {"atr_stop_multiple_multiplier": 1.25}},
                "falsification_question": "Is the result overly dependent on one stop placement?",
            }
        )
    return {
        "schema_version": "experiment_plan_v1",
        "plan_id": f"PLAN-{strategy_spec['strategy_spec_id']}",
        "strategy_spec_id": strategy_spec["strategy_spec_id"],
        "hypothesis_id": strategy_spec["hypothesis_id"],
        "plan_title": f"Falsification plan for {strategy_spec['strategy_spec_id']}",
        "status": "draft",
        "items": items,
        "limits": {
            "max_concurrent": 1,
            "max_queued_items": len(items),
            "estimated_compute_units": sum(int(item["runtime_budget"]["max_variants"]) for item in items),
        },
        "approval_required": True,
    }


def validate_experiment_plan(plan: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in ("schema_version", "plan_id", "strategy_spec_id", "hypothesis_id", "items", "limits"):
        if plan.get(field) in (None, "", [], {}):
            errors.append(f"missing_{field}")
    if plan.get("schema_version") != "experiment_plan_v1":
        errors.append("schema_version_must_be_experiment_plan_v1")
    items = plan.get("items")
    if not isinstance(items, list) or not items:
        errors.append("items_must_be_non_empty_list")
    else:
        seen: set[str] = set()
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                errors.append(f"item_{index}_must_be_object")
                continue
            item_id = str(item.get("item_id") or "")
            if not item_id:
                errors.append(f"item_{index}_missing_item_id")
            if item_id in seen:
                errors.append(f"item_{index}_duplicate_item_id")
            seen.add(item_id)
            if item.get("experiment_type") not in ALLOWED_EXPERIMENT_TYPES:
                errors.append(f"item_{index}_experiment_type_unsupported")
            if not isinstance(item.get("required_datasets"), list) or not item.get("required_datasets"):
                errors.append(f"item_{index}_required_datasets_missing")
            runtime = item.get("runtime_budget")
            if not isinstance(runtime, dict) or int(runtime.get("max_minutes", 0)) <= 0 or int(runtime.get("max_variants", 0)) <= 0:
                errors.append(f"item_{index}_runtime_budget_invalid")
            if not item.get("falsification_question"):
                errors.append(f"item_{index}_missing_falsification_question")
    limits = plan.get("limits")
    if not isinstance(limits, dict) or int(limits.get("max_concurrent", 0)) <= 0 or int(limits.get("max_queued_items", 0)) <= 0:
        errors.append("limits_invalid")
    return errors


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _card_id(card_type: str, strategy_spec_id: str, experiment_item_id: str) -> str:
    raw = f"{CARD_SCHEMA_VERSION}:{strategy_spec_id}:{experiment_item_id}:{card_type}"
    return str(uuid5(NAMESPACE_URL, raw))


def _make_execution_card(
    *,
    card_type: str,
    strategy_spec: dict[str, Any],
    item: dict[str, Any],
    manifest: dict[str, Any],
    verdict: dict[str, Any],
    data: dict[str, Any],
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    if card_type not in B7_CARD_TYPES:
        raise ValueError(f"Unknown B7 card type: {card_type}")
    return {
        "schema_version": CARD_SCHEMA_VERSION,
        "card_type": card_type,
        "card_id": _card_id(card_type, strategy_spec["strategy_spec_id"], item["item_id"]),
        "strategy_spec_id": strategy_spec["strategy_spec_id"],
        "hypothesis_id": strategy_spec["hypothesis_id"],
        "experiment_plan_id": manifest["experiment_plan_id"],
        "experiment_item_id": item["item_id"],
        "experiment_type": item["experiment_type"],
        "title": item["title"],
        "created_at": manifest["generated_at"],
        "source_artifacts": {
            "run_config": "run_config.json",
            "execution_manifest": "execution_manifest.json",
            "verdict": "verdict.json",
            "execution_log": "execution_log.md",
        },
        "data": data,
        "warnings": warnings or [],
    }


def _parameter_summary(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, config in parameters.items():
        if not isinstance(config, dict):
            continue
        out.append(
            {
                "name": name,
                "min": config.get("min"),
                "max": config.get("max"),
                "default": config.get("default"),
                "bounded": isinstance(config.get("min"), (int, float)) and isinstance(config.get("max"), (int, float)),
            }
        )
    return out


def build_contract_intelligence_cards(
    *,
    strategy_spec: dict[str, Any],
    experiment_plan: dict[str, Any],
    item: dict[str, Any],
    manifest: dict[str, Any],
    verdict: dict[str, Any],
    run_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build B7 cards from B6 contract artifacts only.

    The contract runner has not executed market data yet. These cards therefore
    describe protocol readiness, evidence gaps, and the next run decision instead
    of pretending to know realized performance.
    """
    datasets = list(dict.fromkeys(item.get("required_datasets") or strategy_spec.get("required_datasets") or []))
    warnings = [
        "No market-data execution artifacts are present yet; performance, fill, regime, and parameter-stability conclusions are unavailable.",
    ]
    config_patch = item.get("config_patch") or {}
    plan_items = experiment_plan.get("items") if isinstance(experiment_plan.get("items"), list) else []
    has_parameter_grid = any(candidate.get("experiment_type") == "parameter_grid" for candidate in plan_items if isinstance(candidate, dict))
    has_null = any(candidate.get("experiment_type") == "benchmark_null" for candidate in plan_items if isinstance(candidate, dict))
    rich_state_required = bool({"funding", "open_interest", "liquidations", "research_panel"} & set(datasets))
    next_action = (
        "execute_market_data_run"
        if verdict.get("verdict") == "ready_for_engine_execution"
        else "repair_protocol"
    )

    return [
        _make_execution_card(
            card_type="HypothesisCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "thesis_reference": strategy_spec.get("hypothesis_id"),
                "strategy_family": strategy_spec.get("strategy_family"),
                "universe": strategy_spec.get("universe") or [],
                "timeframe": strategy_spec.get("timeframe"),
                "signals": strategy_spec.get("signals") or [],
                "falsification_question": item.get("falsification_question"),
                "declared_assumptions": strategy_spec.get("assistant_assumptions") or [],
            },
            warnings=[],
        ),
        _make_execution_card(
            card_type="RunQualityCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "protocol_valid": True,
                "execution_status": manifest.get("status"),
                "run_config_materialized": True,
                "execution_metrics_available": False,
                "required_datasets": datasets,
                "runtime_budget": item.get("runtime_budget") or {},
                "runtime_limits": manifest.get("runtime_limits") or {},
                "blocking_gaps": verdict.get("blocking_gaps") or [],
            },
            warnings=warnings.copy(),
        ),
        _make_execution_card(
            card_type="ExecutionDragCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "cost_model": run_config.get("cost_model") or {},
                "slippage_model": run_config.get("slippage_model") or {},
                "risk_model": run_config.get("risk_model") or {},
                "execution_semantics": run_config.get("execution_semantics") or {},
                "drag_measured": False,
                "reason": "Market-data execution and trade artifacts are required before execution drag can be measured.",
            },
            warnings=warnings.copy(),
        ),
        _make_execution_card(
            card_type="FailureCauseCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "failure_detected": False,
                "status": manifest.get("status"),
                "root_cause_hint": None,
                "pipeline_log": "execution_log.md",
                "summary": "Contract validation completed. No engine failure was observed by the B6 runner.",
            },
            warnings=[],
        ),
        _make_execution_card(
            card_type="RegimeStateDependencyCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "state_split_requested": item.get("experiment_type") == "regime_state_split" or bool((config_patch.get("state_split") if isinstance(config_patch, dict) else None)),
                "rich_state_required": rich_state_required,
                "required_state_datasets": [dataset for dataset in datasets if dataset in {"funding", "open_interest", "liquidations", "research_panel"}],
                "dependency_measured": False,
                "reason": "Regime/state dependency requires completed run outputs with state-tagged trades or state bucket summaries.",
            },
            warnings=warnings.copy(),
        ),
        _make_execution_card(
            card_type="ParameterFragilityCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "parameter_grid_available_in_plan": has_parameter_grid,
                "this_item_is_parameter_test": item.get("experiment_type") == "parameter_grid",
                "parameters": _parameter_summary(strategy_spec.get("parameters") if isinstance(strategy_spec.get("parameters"), dict) else {}),
                "fragility_measured": False,
                "reason": "Parameter fragility requires completed parameter-grid outputs, not only declared parameter bounds.",
            },
            warnings=warnings.copy(),
        ),
        _make_execution_card(
            card_type="NullComparisonCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "benchmark_or_null_available_in_plan": has_null,
                "this_item_is_null_test": item.get("experiment_type") == "benchmark_null",
                "benchmark_required": "benchmark" in datasets,
                "null_model": config_patch.get("null_model") if isinstance(config_patch, dict) else None,
                "comparison_measured": False,
                "reason": "Benchmark/null comparison requires completed benchmark or matched-null run artifacts.",
            },
            warnings=warnings.copy(),
        ),
        _make_execution_card(
            card_type="VerdictCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "verdict": verdict.get("verdict"),
                "status": verdict.get("status"),
                "confidence": verdict.get("confidence"),
                "summary": verdict.get("summary"),
                "blocking_gaps": verdict.get("blocking_gaps") or [],
                "decision_grade": False,
                "decision_grade_reason": "The current artifact proves protocol readiness, not strategy performance.",
            },
            warnings=warnings.copy(),
        ),
        _make_execution_card(
            card_type="NextExperimentCard",
            strategy_spec=strategy_spec,
            item=item,
            manifest=manifest,
            verdict=verdict,
            data={
                "recommended_action": next_action,
                "next_action": verdict.get("next_action"),
                "requires_human_approval": True,
                "next_inputs_to_review": [
                    "RunQualityCard",
                    "ExecutionDragCard",
                    "RegimeStateDependencyCard",
                    "ParameterFragilityCard",
                    "NullComparisonCard",
                ],
            },
            warnings=[],
        ),
    ]


def render_contract_cards_markdown(cards: list[dict[str, Any]]) -> str:
    lines = ["# Experiment Verdict Cards", ""]
    for card in cards:
        data = card.get("data") if isinstance(card.get("data"), dict) else {}
        lines.extend(
            [
                f"## {card['card_type']}",
                "",
                f"- Strategy spec: `{card.get('strategy_spec_id')}`",
                f"- Experiment item: `{card.get('experiment_item_id')}`",
                f"- Status: `{data.get('status') or data.get('execution_status') or 'recorded'}`",
            ]
        )
        if card.get("warnings"):
            lines.append(f"- Warnings: {len(card['warnings'])}")
        summary = data.get("summary") or data.get("reason") or data.get("decision_grade_reason") or data.get("next_action")
        if summary:
            lines.append(f"- Summary: {summary}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_contract_intelligence_cards(
    *,
    output_dir: Path,
    cards: list[dict[str, Any]],
) -> dict[str, Any]:
    card_paths: dict[str, str] = {}
    for card in cards:
        path = output_dir / f"{card['card_type']}.json"
        path.write_text(json.dumps(card, indent=2, sort_keys=True), encoding="utf-8")
        card_paths[card["card_type"]] = path.name
    bundle = {
        "schema_version": CARD_BUNDLE_SCHEMA_VERSION,
        "card_schema_version": CARD_SCHEMA_VERSION,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cards": cards,
    }
    (output_dir / "cards.json").write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "cards.md").write_text(render_contract_cards_markdown(cards), encoding="utf-8")
    return {"bundle": bundle, "card_paths": card_paths, "bundle_json": "cards.json", "bundle_markdown": "cards.md"}


def execute_experiment_contract(
    *,
    strategy_spec: dict[str, Any],
    experiment_plan: dict[str, Any],
    item_id: str,
    output_dir: Path,
    runtime_limits: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strategy_errors = validate_strategy_spec(strategy_spec)
    plan_errors = validate_experiment_plan(experiment_plan)
    if strategy_errors or plan_errors:
        raise SpecValidationError([*strategy_errors, *plan_errors])
    item = next((candidate for candidate in experiment_plan["items"] if candidate.get("item_id") == item_id), None)
    if not item:
        raise SpecValidationError([f"experiment_item_not_found_{item_id}"])
    if item.get("enabled") is False:
        raise SpecValidationError([f"experiment_item_disabled_{item_id}"])

    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
    run_config = compile_strategy_run_config(strategy_spec)
    patched_run_config = _deep_merge(run_config, item.get("config_patch") or {})
    runtime_limits = runtime_limits or {}
    manifest = {
        "schema_version": "experiment_execution_manifest_v1",
        "generated_at": generated_at,
        "status": "contract_ready",
        "strategy_spec_id": strategy_spec["strategy_spec_id"],
        "hypothesis_id": strategy_spec["hypothesis_id"],
        "experiment_plan_id": experiment_plan["plan_id"],
        "experiment_item_id": item["item_id"],
        "experiment_type": item["experiment_type"],
        "title": item["title"],
        "falsification_question": item["falsification_question"],
        "required_datasets": item["required_datasets"],
        "runtime_budget": item["runtime_budget"],
        "runtime_limits": runtime_limits,
        "config_patch": item.get("config_patch") or {},
        "artifacts": {
            "run_config": "run_config.json",
            "execution_manifest": "execution_manifest.json",
            "verdict": "verdict.json",
            "log": "execution_log.md",
            "cards_json": "cards.json",
            "cards_markdown": "cards.md",
        },
        "limitations": [
            "B6 contract execution validates and materializes the approved run protocol.",
            "Market-data backtest execution requires the tenant-safe daemon service mode and data profile binding.",
        ],
    }
    verdict = {
        "schema_version": "experiment_verdict_v1",
        "status": "contract_ready",
        "verdict": "ready_for_engine_execution",
        "confidence": "protocol_validated",
        "summary": "The strategy spec and experiment item are valid and have been materialized into an auditable run config.",
        "blocking_gaps": [
            "No tenant-safe market-data execution daemon was invoked by this contract runner."
        ],
        "next_action": "Bind a data profile and execute through the production research daemon service.",
    }
    log = "\n".join(
        [
            "# Experiment Execution Log",
            "",
            f"- Generated: {generated_at}",
            f"- Strategy spec: {strategy_spec['strategy_spec_id']}",
            f"- Hypothesis: {strategy_spec['hypothesis_id']}",
            f"- Experiment item: {item['item_id']} ({item['experiment_type']})",
            f"- Falsification question: {item['falsification_question']}",
            "",
            "## Result",
            "",
            "Contract validation passed. The run config has been materialized for the execution daemon.",
            "",
        ]
    )

    (output_dir / "run_config.json").write_text(json.dumps(patched_run_config, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "execution_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "verdict.json").write_text(json.dumps(verdict, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "execution_log.md").write_text(log, encoding="utf-8")
    cards = build_contract_intelligence_cards(
        strategy_spec=strategy_spec,
        experiment_plan=experiment_plan,
        item=item,
        manifest=manifest,
        verdict=verdict,
        run_config=patched_run_config,
    )
    card_bundle = write_contract_intelligence_cards(output_dir=output_dir, cards=cards)
    return {"manifest": manifest, "verdict": verdict, "run_config": patched_run_config, "cards": card_bundle["bundle"]}


def _print_result(kind: str, path: Path, errors: list[str]) -> int:
    if errors:
        print(json.dumps({"kind": kind, "path": str(path), "valid": False, "errors": errors}, indent=2))
        return 1
    print(json.dumps({"kind": kind, "path": str(path), "valid": True, "errors": []}, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bt")
    sub = parser.add_subparsers(dest="domain", required=True)
    hyp = sub.add_parser("hypothesis")
    hyp_sub = hyp.add_subparsers(dest="action", required=True)
    hyp_validate = hyp_sub.add_parser("validate")
    hyp_validate.add_argument("spec")
    hyp_missing = hyp_sub.add_parser("explain-missing")
    hyp_missing.add_argument("spec")
    strat = sub.add_parser("strategy")
    strat_sub = strat.add_subparsers(dest="action", required=True)
    strat_validate = strat_sub.add_parser("validate")
    strat_validate.add_argument("spec")
    strat_compile = strat_sub.add_parser("compile")
    strat_compile.add_argument("spec")
    exp = sub.add_parser("experiment")
    exp_sub = exp.add_subparsers(dest="action", required=True)
    exp_plan = exp_sub.add_parser("plan")
    exp_plan.add_argument("strategy_spec")
    exp_validate = exp_sub.add_parser("validate")
    exp_validate.add_argument("plan")
    exp_execute = exp_sub.add_parser("execute")
    exp_execute.add_argument("--strategy-spec", required=True)
    exp_execute.add_argument("--experiment-plan", required=True)
    exp_execute.add_argument("--item-id", required=True)
    exp_execute.add_argument("--output-dir", required=True)
    exp_execute.add_argument("--runtime-limits-json")
    args = parser.parse_args(argv)

    if args.domain == "experiment" and args.action == "execute":
        strategy_path = Path(args.strategy_spec)
        plan_path = Path(args.experiment_plan)
        runtime_limits = json.loads(args.runtime_limits_json) if args.runtime_limits_json else {}
        result = execute_experiment_contract(
            strategy_spec=load_spec(strategy_path),
            experiment_plan=load_spec(plan_path),
            item_id=args.item_id,
            output_dir=Path(args.output_dir),
            runtime_limits=runtime_limits,
        )
        print(json.dumps({"kind": "experiment_execution_v1", "valid": True, "output_dir": args.output_dir, **result}, indent=2, sort_keys=True))
        return 0

    spec_arg = getattr(args, "spec", None) or getattr(args, "strategy_spec", None) or getattr(args, "plan", None)
    path = Path(spec_arg)
    spec = load_spec(path)
    if args.domain == "hypothesis" and args.action == "validate":
        return _print_result("hypothesis_spec_v1", path, validate_hypothesis_spec(spec))
    if args.domain == "hypothesis" and args.action == "explain-missing":
        missing = explain_missing_hypothesis_spec(spec)
        print(json.dumps({"kind": "hypothesis_spec_v1", "path": str(path), "missing": missing}, indent=2))
        return 1 if missing else 0
    if args.domain == "strategy" and args.action == "validate":
        return _print_result("strategy_spec_v1", path, validate_strategy_spec(spec))
    if args.domain == "strategy" and args.action == "compile":
        print(json.dumps(compile_strategy_run_config(spec), indent=2, sort_keys=True))
        return 0
    if args.domain == "experiment" and args.action == "plan":
        print(json.dumps(build_experiment_plan(spec), indent=2, sort_keys=True))
        return 0
    if args.domain == "experiment" and args.action == "validate":
        return _print_result("experiment_plan_v1", path, validate_experiment_plan(spec))
    parser.error("unsupported command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
