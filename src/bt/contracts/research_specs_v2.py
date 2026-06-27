"""Confirmed Hypothesis Card to truth-governed engine artifact bridge."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

import yaml

from bt.strategy import STRATEGY_REGISTRY


CARD_VERSION = "hypothesis_card_v1"
HYPOTHESIS_VERSION = "hypothesis_spec_v2"
ENGINE_YAML_VERSION = "engine_hypothesis_yaml_v1"
STRATEGY_VERSION = "strategy_spec_v2"
READINESS_VERSION = "compile_readiness_report_v1"
IR_VERSION = "research_strategy_ir_v1"
COMPILER_VERSION = "research_graph_compiler_v1"
READINESS_STATES = {"registry_ready", "graph_compilable", "implementation_required", "data_blocked", "semantics_blocked", "unsupported"}
DATASETS = {"ohlcv", "trades", "funding", "open_interest", "mark_price", "index_price", "liquidations", "benchmark", "research_panel"}
PORTABLE_TRANSFORMS = {"identity", "sma", "ema", "atr", "true_range", "return", "zscore", "percentile_rank", "half_range_over_close", "true_range_over"}
PORTABLE_OPS = {">", ">=", "<", "<=", "=="}
EXACT_TRUTH = {
    "strict_utc": True, "missing_bars": "no_decision", "interpolation": "forbidden",
    "htf_completeness": "closed_only", "aux_join_direction": "backward",
    "execution_authority": "engine", "risk_authority": "engine", "accounting": "engine_canonical_R",
}
ENGINE_TRUTH_BLOCK = {
    "version": "1.0", "profile": "production", "no_lookahead": True, **EXACT_TRUTH,
    "truth_gate_required": True, "parity_required_for_fast_path": True,
    "research_memory_requires_certification": True,
}


class ResearchSpecV2Error(ValueError):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(";".join(errors))


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_hash(payload: Any) -> str:
    return sha256(canonical_json(payload).encode()).hexdigest()


def validate_hypothesis_card(card: dict[str, Any], *, require_confirmed: bool = True) -> list[str]:
    errors: list[str] = []
    required = (
        "card_id", "program_id", "version", "title", "claim", "intuition", "market_mechanism",
        "features", "gates", "entry", "exit", "sizing", "risk_controls", "parameters",
        "data_requirements", "logging_requirements", "evaluation", "falsification_criteria",
        "expected_failure_modes", "execution_semantics", "field_provenance",
    )
    if card.get("schema_version") != CARD_VERSION:
        errors.append("card_schema_version_invalid")
    errors.extend(f"card_missing_{field}" for field in required if card.get(field) in (None, "", [], {}))
    if require_confirmed and card.get("status") != "confirmed":
        errors.append("card_must_be_confirmed")
    if require_confirmed and (not card.get("confirmed_by") or not card.get("confirmed_at")):
        errors.append("card_confirmation_identity_required")
    for dataset in card.get("data_requirements", []) if isinstance(card.get("data_requirements"), list) else []:
        if dataset not in DATASETS:
            errors.append(f"card_dataset_unsupported_{dataset}")
    semantics = card.get("execution_semantics") if isinstance(card.get("execution_semantics"), dict) else {}
    for key, expected in EXACT_TRUTH.items():
        if semantics.get(key) != expected:
            errors.append(f"card_execution_semantics_{key}_must_equal_{expected}")
    for index, feature in enumerate(card.get("features", []) if isinstance(card.get("features"), list) else []):
        if not isinstance(feature, dict) or not feature.get("id") or not feature.get("source") or not feature.get("transform"):
            errors.append(f"card_feature_{index}_invalid")
        if isinstance(feature, dict) and int(feature.get("lag", 0)) < 0:
            errors.append(f"card_feature_{index}_future_lag_forbidden")
        if isinstance(feature, dict) and feature.get("join") not in (None, "backward"):
            errors.append(f"card_feature_{index}_join_must_be_backward")
    for index, gate in enumerate(card.get("gates", []) if isinstance(card.get("gates"), list) else []):
        if not isinstance(gate, dict) or gate.get("op") not in PORTABLE_OPS:
            errors.append(f"card_gate_{index}_invalid")
    provenance = card.get("field_provenance") if isinstance(card.get("field_provenance"), dict) else {}
    for field in ("claim", "entry", "exit"):
        state = provenance.get(field, {}).get("state") if isinstance(provenance.get(field), dict) else None
        if state in {"unresolved", "unsupported", "inferred", "recommended"}:
            errors.append(f"card_blocking_field_{field}_not_confirmed")
    return errors


def normalize_card(card: dict[str, Any]) -> dict[str, Any]:
    errors = validate_hypothesis_card(card)
    if errors:
        raise ResearchSpecV2Error(errors)
    normalized = {
        "schema_version": IR_VERSION,
        "card_id": card["card_id"], "card_version": card["version"], "card_hash": canonical_hash(card),
        "program_id": card["program_id"], "title": card["title"], "claim": card["claim"],
        "mechanism": card["market_mechanism"], "engine_strategy_name": card.get("engine_strategy_name"),
        "engine_hypothesis_template": card.get("engine_hypothesis_template"),
        "feature_graph": sorted(deepcopy(card["features"]), key=lambda item: str(item["id"])),
        "gate_graph": deepcopy(card["gates"]), "entry": deepcopy(card["entry"]), "exit": deepcopy(card["exit"]),
        "sizing": deepcopy(card["sizing"]), "risk_controls": deepcopy(card["risk_controls"]),
        "parameters": {key: deepcopy(card["parameters"][key]) for key in sorted(card["parameters"])},
        "data_requirements": sorted(set(card["data_requirements"])),
        "logging_requirements": sorted(set(card["logging_requirements"])),
        "evaluation": deepcopy(card["evaluation"]), "falsification_criteria": deepcopy(card["falsification_criteria"]),
        "expected_failure_modes": deepcopy(card["expected_failure_modes"]), "execution_semantics": deepcopy(card["execution_semantics"]),
        "source_citations": deepcopy(card.get("source_citations", [])), "field_provenance": deepcopy(card["field_provenance"]),
    }
    return normalized


def _default_parameters(parameters: dict[str, list[Any]]) -> dict[str, Any]:
    return {key: values[0] for key, values in parameters.items() if isinstance(values, list) and values}


def build_hypothesis_spec_v2(ir: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": HYPOTHESIS_VERSION, "hypothesis_id": ir["card_id"], "program_id": ir["program_id"],
        "title": ir["title"], "claim": ir["claim"], "market_mechanism": ir["mechanism"],
        "observable_features": [item["id"] for item in ir["feature_graph"]], "required_datasets": ir["data_requirements"],
        "invalidation_criteria": ir["falsification_criteria"], "expected_failure_modes": ir["expected_failure_modes"],
        "parameter_grid": ir["parameters"], "evaluation": ir["evaluation"], "execution_semantics": ir["execution_semantics"],
        "source_card_hash": ir["card_hash"], "source_citations": ir["source_citations"], "field_provenance": ir["field_provenance"],
    }


def build_engine_hypothesis_yaml(ir: dict[str, Any], *, repo_root: str | Path = ".") -> dict[str, Any]:
    template = ir.get("engine_hypothesis_template")
    if template:
        root = Path(repo_root).resolve()
        path = (root / str(template)).resolve()
        allowed = (root / "research" / "hypotheses").resolve()
        if allowed not in path.parents or not path.exists():
            raise ResearchSpecV2Error(["engine_hypothesis_template_invalid"])
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ResearchSpecV2Error(["engine_hypothesis_template_invalid"])
        template_strategy = payload.get("entry", {}).get("strategy") if isinstance(payload.get("entry"), dict) else None
        if template_strategy != ir.get("engine_strategy_name"):
            raise ResearchSpecV2Error(["engine_hypothesis_template_strategy_mismatch"])
        template_grid = payload.get("parameter_grid") if isinstance(payload.get("parameter_grid"), dict) else {}
        mismatched = [key for key, values in ir["parameters"].items() if key not in template_grid or template_grid[key] != values]
        if mismatched:
            raise ResearchSpecV2Error([f"engine_hypothesis_template_parameter_mismatch_{key}" for key in mismatched])
    else:
        strategy_name = ir.get("engine_strategy_name") or "research_graph_v1"
        signal_tf = str(ir["execution_semantics"].get("signal_timeframe", "15m"))
        payload = {
            "hypothesis_id": ir["card_id"], "title": ir["title"], "description": ir["claim"],
            "research_layer": "generated", "hypothesis_family": "portable_research_graph", "version": "1.0.0",
            "required_indicators": [], "indicator_defaults": {}, "parameter_grid": ir["parameters"],
            "gates": ir["gate_graph"], "entry": {**ir["entry"], "strategy": strategy_name, "signal_timeframe": signal_tf},
            "exit": ir["exit"], "execution_semantics": ir["execution_semantics"],
            "evaluation": {"required_tiers": ir["evaluation"].get("tiers", ["Tier2", "Tier3"]), "metrics": ir["evaluation"].get("metrics", [])},
            "logging": {"schema_version": "1.0", "required_fields": ir["logging_requirements"]},
            "runtime_controls": {"enabled": True, "max_variants": 128, "tags": ["generated", "confirmed_card"]},
            "notes": {"falsification_criteria": ir["falsification_criteria"], "failure_modes": ir["expected_failure_modes"]},
        }
    payload["truth_contract"] = deepcopy(ENGINE_TRUTH_BLOCK)
    payload["generation_provenance"] = {"schema_version": ENGINE_YAML_VERSION, "source_card_hash": ir["card_hash"], "compiler_version": COMPILER_VERSION}
    return payload


def build_strategy_spec_v2(ir: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": STRATEGY_VERSION, "strategy_spec_id": f"strategy-{ir['card_id']}", "program_id": ir["program_id"],
        "hypothesis_id": ir["card_id"], "engine_strategy_name": ir.get("engine_strategy_name"),
        "feature_graph": ir["feature_graph"], "gate_graph": ir["gate_graph"], "entry": ir["entry"], "exit_state_machine": ir["exit"],
        "sizing": ir["sizing"], "risk_policy": ir["risk_controls"], "parameter_grid": ir["parameters"],
        "parameter_defaults": _default_parameters(ir["parameters"]), "data_requirements": ir["data_requirements"],
        "logging_contract": ir["logging_requirements"], "evaluation_contract": ir["evaluation"],
        "falsification_contract": ir["falsification_criteria"], "execution_semantics": ir["execution_semantics"],
        "truth_contract": deepcopy(ENGINE_TRUTH_BLOCK), "source_card_hash": ir["card_hash"], "compiler_version": COMPILER_VERSION,
        "user_approval_required": True,
    }


def compile_readiness(spec: dict[str, Any], *, available_datasets: set[str] | None = None) -> dict[str, Any]:
    blockers: list[dict[str, str]] = []
    available = available_datasets if available_datasets is not None else set(spec.get("data_requirements", []))
    missing_data = sorted(set(spec.get("data_requirements", [])) - available)
    if missing_data:
        blockers.extend({"code": "dataset_missing", "detail": item} for item in missing_data)
        status = "data_blocked"
    else:
        semantics = spec.get("execution_semantics", {})
        bad_semantics = [key for key, value in EXACT_TRUTH.items() if semantics.get(key) != value]
        if bad_semantics:
            blockers.extend({"code": "semantic_mismatch", "detail": item} for item in bad_semantics)
            status = "semantics_blocked"
        elif spec.get("engine_strategy_name") in STRATEGY_REGISTRY:
            status = "registry_ready"
        else:
            unsupported_features = [item.get("transform") for item in spec.get("feature_graph", []) if item.get("transform") not in PORTABLE_TRANSFORMS]
            unsupported_gates = [item.get("op") for item in spec.get("gate_graph", []) if item.get("op") not in PORTABLE_OPS]
            feature_ids = {str(item.get("id")) for item in spec.get("feature_graph", [])}
            parameter_ids = set(spec.get("parameter_grid", {}))
            invalid_gate_refs = [str(item.get("left", item.get("field"))) for item in spec.get("gate_graph", []) if str(item.get("left", item.get("field"))) not in feature_ids]
            invalid_gate_params = [str(item.get("right_param", item.get("param"))) for item in spec.get("gate_graph", []) if item.get("right") is None and str(item.get("right_param", item.get("param"))) not in parameter_ids]
            exit_type = spec.get("exit_state_machine", {}).get("type")
            auxiliary_sources = sorted({str(item.get("source")) for item in spec.get("feature_graph", []) if item.get("source") not in {"ohlcv", "derived"}})
            sizing = spec.get("sizing", {})
            sizing_supported = sizing.get("mode") == "constant_r" and sizing.get("stop_required") is True
            logging = set(spec.get("logging_contract", []))
            logging_supported = {"decision_trace", "stop_price"}.issubset(logging)
            if unsupported_features or unsupported_gates or invalid_gate_refs or invalid_gate_params or auxiliary_sources or exit_type != "fixed_stop_time_exit" or not sizing_supported or not logging_supported:
                blockers.extend({"code": "primitive_missing", "detail": str(item)} for item in [*unsupported_features, *unsupported_gates])
                blockers.extend({"code": "feature_reference_missing", "detail": item} for item in invalid_gate_refs)
                blockers.extend({"code": "parameter_reference_missing", "detail": item} for item in invalid_gate_params)
                blockers.extend({"code": "auxiliary_source_requires_implementation", "detail": item} for item in auxiliary_sources)
                if exit_type != "fixed_stop_time_exit":
                    blockers.append({"code": "exit_not_portable", "detail": str(exit_type)})
                if not sizing_supported:
                    blockers.append({"code": "sizing_not_portable", "detail": str(sizing.get("mode"))})
                if not logging_supported:
                    blockers.append({"code": "logging_contract_incomplete", "detail": "decision_trace,stop_price"})
                status = "implementation_required"
            else:
                status = "graph_compilable"
    return {
        "schema_version": READINESS_VERSION, "strategy_spec_id": spec.get("strategy_spec_id"), "status": status,
        "blockers": blockers, "capabilities": {
            "registered_strategy": spec.get("engine_strategy_name") in STRATEGY_REGISTRY,
            "portable_feature_graph": not any(item.get("transform") not in PORTABLE_TRANSFORMS for item in spec.get("feature_graph", [])),
            "truth_contract_valid": not any(key for key, value in EXACT_TRUTH.items() if spec.get("execution_semantics", {}).get(key) != value),
            "engine_owned_risk": spec.get("execution_semantics", {}).get("risk_authority") == "engine",
            "rich_logging": {"decision_trace", "stop_price"}.issubset(set(spec.get("logging_contract", []))),
        }, "compiler_version": COMPILER_VERSION, "source_card_hash": spec.get("source_card_hash"),
    }


def compile_strategy_spec_v2(spec: dict[str, Any], readiness: dict[str, Any]) -> dict[str, Any]:
    if readiness.get("status") not in {"registry_ready", "graph_compilable"}:
        raise ResearchSpecV2Error([f"strategy_not_compilable_{readiness.get('status')}"])
    strategy_name = spec.get("engine_strategy_name") if readiness["status"] == "registry_ready" else "research_graph_v1"
    return {
        "schema_version": "run_config_from_strategy_spec_v2", "strategy_spec_id": spec["strategy_spec_id"],
        "strategy_spec_hash": canonical_hash(spec), "strategy": {"name": strategy_name, "parameters": spec["parameter_defaults"], "research_graph": {"features": spec["feature_graph"], "gates": spec["gate_graph"], "entry": spec["entry"], "exit": spec["exit_state_machine"]}},
        "data": {"dataset_kind": "research_panel" if "research_panel" in spec["data_requirements"] else "ohlcv"},
        "required_datasets": spec["data_requirements"], "risk": spec["risk_policy"], "execution_semantics": spec["execution_semantics"],
        "truth_contract": spec["truth_contract"], "compiler": {"version": COMPILER_VERSION, "status": readiness["status"]},
    }


def build_implementation_task(spec: dict[str, Any], readiness: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "strategy_implementation_task_v1", "task_id": f"implementation-{spec['strategy_spec_id']}",
        "strategy_spec_id": spec["strategy_spec_id"], "strategy_spec_hash": canonical_hash(spec), "source_card_hash": spec["source_card_hash"],
        "status": "draft", "blockers": readiness["blockers"],
        "required_deliverables": ["strategy_module", "strategy_registry_entry", "feature_kernel", "hypothesis_yaml", "hypothesis_documentation", "contract_tests", "integration_smoke"],
        "required_evidence": ["admission_PASS", "lookahead_tests", "determinism_tests", "rich_logging_tests", "OHLCV_fallback_smoke", "enriched_data_smoke", "stable_membership_smoke", "volatile_membership_smoke", "classic_fast_parity_or_classic_only", "experiment_truth_PASS"],
        "prohibited_shortcuts": ["future_values", "forward_aux_join", "interpolation", "strategy_owned_execution", "strategy_owned_risk", "uncertified_result_publication"],
        "approval_required": True,
    }


def build_artifact_bundle(card: dict[str, Any], *, repo_root: str | Path = ".", available_datasets: set[str] | None = None) -> dict[str, Any]:
    ir = normalize_card(card)
    hypothesis = build_hypothesis_spec_v2(ir)
    engine_yaml = build_engine_hypothesis_yaml(ir, repo_root=repo_root)
    strategy = build_strategy_spec_v2(ir)
    readiness = compile_readiness(strategy, available_datasets=available_datasets)
    bundle = {
        "schema_version": "research_spec_bundle_v2", "card": card, "normalized_ir": ir,
        "hypothesis_spec": hypothesis, "engine_hypothesis_yaml": engine_yaml, "strategy_spec": strategy,
        "compile_readiness": readiness,
    }
    if readiness["status"] == "implementation_required":
        bundle["implementation_task"] = build_implementation_task(strategy, readiness)
    if readiness["status"] in {"registry_ready", "graph_compilable"}:
        bundle["run_config"] = compile_strategy_spec_v2(strategy, readiness)
    return bundle


def write_artifact_bundle(bundle: dict[str, Any], output_dir: str | Path) -> dict[str, str]:
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    mapping = {
        "hypothesis_spec": "hypothesis_spec_v2.json", "strategy_spec": "strategy_spec_v2.json",
        "compile_readiness": "compile_readiness_report_v1.json", "normalized_ir": "research_strategy_ir_v1.json",
    }
    for key, name in mapping.items():
        path = target / name
        path.write_text(json.dumps(bundle[key], indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs[key] = str(path)
    yaml_path = target / "engine_hypothesis_yaml_v1.yaml"
    yaml_path.write_text(yaml.safe_dump(bundle["engine_hypothesis_yaml"], sort_keys=False), encoding="utf-8")
    outputs["engine_hypothesis_yaml"] = str(yaml_path)
    if "implementation_task" in bundle:
        path = target / "strategy_implementation_task_v1.json"
        path.write_text(json.dumps(bundle["implementation_task"], indent=2, sort_keys=True) + "\n", encoding="utf-8")
        outputs["implementation_task"] = str(path)
    return outputs


def upgrade_hypothesis_spec_v1(spec: dict[str, Any]) -> dict[str, Any]:
    """Read the legacy SaaS hypothesis shape without pretending it is executable."""
    if spec.get("schema_version") == HYPOTHESIS_VERSION:
        return deepcopy(spec)
    if spec.get("schema_version") != "hypothesis_spec_v1":
        raise ResearchSpecV2Error(["hypothesis_spec_version_unsupported"])
    return {
        "schema_version": HYPOTHESIS_VERSION,
        "hypothesis_id": spec.get("hypothesis_id"),
        "program_id": spec.get("program_id"),
        "title": spec.get("title"),
        "claim": spec.get("claim"),
        "market_mechanism": spec.get("market_mechanism", "Unspecified in V1"),
        "observable_features": spec.get("observable_features", []),
        "required_datasets": spec.get("required_datasets", []),
        "invalidation_criteria": spec.get("invalidation_criteria", []),
        "expected_failure_modes": spec.get("expected_failure_modes", []),
        "parameter_grid": spec.get("parameter_grid", {}),
        "evaluation": spec.get("evaluation", {}),
        "execution_semantics": deepcopy(EXACT_TRUTH),
        "source_card_hash": None,
        "source_citations": [],
        "field_provenance": {"legacy": {"state": "extracted", "confidence": 1.0}},
        "compatibility": {"upgraded_from": "hypothesis_spec_v1", "execution_status": "requires_card_confirmation"},
    }


def upgrade_strategy_spec_v1(spec: dict[str, Any]) -> dict[str, Any]:
    """Convert legacy strategy specs into review-only V2 records."""
    if spec.get("schema_version") == STRATEGY_VERSION:
        return deepcopy(spec)
    if spec.get("schema_version") != "strategy_spec_v1":
        raise ResearchSpecV2Error(["strategy_spec_version_unsupported"])
    return {
        "schema_version": STRATEGY_VERSION,
        "strategy_spec_id": spec.get("strategy_spec_id"),
        "program_id": spec.get("program_id"),
        "hypothesis_id": spec.get("hypothesis_id"),
        "engine_strategy_name": spec.get("engine_strategy_name"),
        "feature_graph": spec.get("feature_graph", []),
        "gate_graph": spec.get("gate_graph", []),
        "entry": spec.get("entry", {}),
        "exit_state_machine": spec.get("exit_state_machine", spec.get("exit", {})),
        "sizing": spec.get("sizing", {}),
        "risk_policy": spec.get("risk_policy", {}),
        "parameter_grid": spec.get("parameter_grid", {}),
        "parameter_defaults": spec.get("parameter_defaults", {}),
        "data_requirements": spec.get("data_requirements", []),
        "logging_contract": spec.get("logging_contract", []),
        "evaluation_contract": spec.get("evaluation_contract", {}),
        "falsification_contract": spec.get("falsification_contract", []),
        "execution_semantics": deepcopy(EXACT_TRUTH),
        "truth_contract": deepcopy(ENGINE_TRUTH_BLOCK),
        "source_card_hash": None,
        "compiler_version": COMPILER_VERSION,
        "user_approval_required": True,
        "compatibility": {"upgraded_from": "strategy_spec_v1", "execution_status": "requires_card_confirmation"},
    }
