"""Deterministic feasibility and scaffolding for governed alpha strategies."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

import yaml


SCHEMA_VERSION = "strategy-intent-v1.0.0"
SCAFFOLD_VERSION = "strategy-scaffold-v1.0.0"
RESAMPLING_POLICY = "left_closed_left_labeled_complete_bars"
TRUTH_CONTRACT = {
    "version": "1.0",
    "profile": "production",
    "no_lookahead": True,
    "strict_utc": True,
    "missing_bars": "no_decision",
    "interpolation": "forbidden",
    "htf_completeness": "closed_only",
    "aux_join_direction": "backward",
    "execution_authority": "engine",
    "risk_authority": "engine",
    "accounting": "engine_canonical_R",
    "truth_gate_required": True,
    "parity_required_for_fast_path": True,
    "fast_path_generation": "forbidden",
    "research_memory_requires_certification": True,
}
IDENTITY_FIELDS = (
    "dataset_build_id",
    "dataset_digest",
    "catalog_digest",
    "manifest_digest",
    "producer_receipt_digest",
    "lake_governance_digest",
)


class StrategyIntentError(ValueError):
    """The research handoff is not feasible for bounded strategy authoring."""


def canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def digest(value: object) -> str:
    return sha256(canonical(value)).hexdigest()


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    if not normalized:
        raise StrategyIntentError("candidate key cannot produce a strategy identifier")
    return f"alpha_003_{normalized[:100]}"


def _parse_time(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise StrategyIntentError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise StrategyIntentError(f"{field} must be timezone aware")
    return parsed


@dataclass(frozen=True)
class StrategyIntent:
    schema_version: str
    intent_id: str
    strategy_name: str
    hypothesis_id: str
    question: str
    question_digest: str
    predictor: str
    target: str
    horizon: str
    predicted_direction: str
    causal_timing: str
    null_hypothesis: str
    mechanism: str
    rival_explanations: list[str]
    falsification_criteria: list[str]
    parameter_names: list[str]
    maximum_variants: int
    venue: str
    primary_instrument: str
    instruments: list[str]
    source_timeframe: str
    research_timeframe: str
    resampling_policy: str
    window: dict[str, str]
    dataset_bindings: list[dict[str, Any]]
    representation_plan: dict[str, Any]
    authority: dict[str, bool]
    unresolved_authoring: list[str]
    evidence_digest: str

    def document(self) -> dict[str, Any]:
        value = asdict(self)
        value["intent_digest"] = digest(value)
        return value


def compile_strategy_intent(evidence: dict[str, Any]) -> StrategyIntent:
    """Compile a fail-closed native intent without interpreting market prose."""
    if not isinstance(evidence, dict):
        raise StrategyIntentError("engineering evidence must be an object")
    candidate = evidence.get("discovery_candidate")
    if not isinstance(candidate, dict):
        raise StrategyIntentError("structured discovery candidate is required")
    question = " ".join(str(evidence.get("question", "")).split())
    if question != " ".join(str(candidate.get("question", "")).split()):
        raise StrategyIntentError(
            "candidate question differs from engineering question"
        )
    expected_question_digest = digest({"question": question})
    if evidence.get("question_digest") != expected_question_digest:
        raise StrategyIntentError("question digest is invalid")

    required_text = (
        "candidate_key",
        "predictor",
        "target",
        "horizon",
        "causal_timing",
        "null_hypothesis",
        "mechanism",
        "predicted_direction",
    )
    missing = [key for key in required_text if not str(candidate.get(key, "")).strip()]
    if missing:
        raise StrategyIntentError(f"candidate fields are missing: {','.join(missing)}")
    if not re.search(
        r"\b[1-9][0-9]*\s*(?:m|min|minute|h|hour|d|day)s?\b",
        str(candidate["horizon"]),
        re.I,
    ):
        raise StrategyIntentError("candidate horizon is not machine-readable")
    if candidate["predicted_direction"] not in {
        "positive",
        "negative",
        "nonlinear",
        "conditional",
    }:
        raise StrategyIntentError("candidate direction is unsupported")

    data = candidate.get("data")
    plan = evidence.get("representation_plan")
    if not isinstance(data, dict) or not isinstance(plan, dict):
        raise StrategyIntentError("typed data and representation plans are required")
    instruments = [str(item).upper() for item in data.get("instruments", [])]
    if not instruments or len(instruments) != len(set(instruments)):
        raise StrategyIntentError("candidate instruments must be present and unique")
    if str(data.get("instrument", "")).upper() not in instruments:
        raise StrategyIntentError("primary instrument is outside the candidate basket")
    if plan.get("instruments") != instruments:
        raise StrategyIntentError("representation basket differs from candidate data")
    if data.get("timeframe") != "1m" or plan.get("source_timeframe") != "1m":
        raise StrategyIntentError("only admitted 1m source data may be transformed")
    if (
        data.get("resampling_policy") != RESAMPLING_POLICY
        or plan.get("resampling_policy") != RESAMPLING_POLICY
    ):
        raise StrategyIntentError("resampling policy is not canonical")
    if data.get("research_timeframe") != plan.get("research_timeframe"):
        raise StrategyIntentError("research timeframe differs from representation plan")
    if plan.get("outcome_data_consulted") is not False:
        raise StrategyIntentError("representation selection consulted outcomes")
    if plan.get("selection_data_boundary") != "metadata_predictors_only_no_targets":
        raise StrategyIntentError("representation selection boundary is invalid")
    transformations = plan.get("transformations")
    if not isinstance(transformations, list) or not transformations:
        raise StrategyIntentError("representation plan has no transformations")
    outputs = [
        item.get("output_field") for item in transformations if isinstance(item, dict)
    ]
    if len(outputs) != len(transformations) or len(outputs) != len(set(outputs)):
        raise StrategyIntentError(
            "representation output fields are incomplete or duplicated"
        )

    bindings = evidence.get("dataset_bindings")
    if not isinstance(bindings, list) or not bindings:
        raise StrategyIntentError("dataset bindings are required")
    binding_instruments = []
    for item in bindings:
        if not isinstance(item, dict):
            raise StrategyIntentError("dataset binding must be an object")
        absent = [field for field in IDENTITY_FIELDS if not item.get(field)]
        if absent:
            raise StrategyIntentError(
                f"dataset binding identity is incomplete: {','.join(absent)}"
            )
        partitions = item.get("partition_digests")
        if not isinstance(partitions, list) or not partitions:
            raise StrategyIntentError(
                "dataset binding partition identity is incomplete"
            )
        bound_instruments = item.get("instruments")
        if isinstance(bound_instruments, list) and bound_instruments:
            binding_instruments.extend(
                str(value).upper() for value in bound_instruments
            )
        else:
            binding_instruments.append(str(item.get("instrument", "")).upper())
    if sorted(binding_instruments) != sorted(instruments):
        raise StrategyIntentError("dataset bindings do not cover the exact basket")

    window = evidence.get("window")
    if not isinstance(window, dict):
        raise StrategyIntentError("execution window is required")
    start = _parse_time(window.get("start"), "window.start")
    end = _parse_time(window.get("end"), "window.end")
    if end <= start or (end - start).days < 365:
        raise StrategyIntentError("execution window must contain at least 365 days")

    budget = candidate.get("parameter_budget")
    if not isinstance(budget, dict):
        raise StrategyIntentError("parameter budget is required")
    names = budget.get("parameter_names")
    maximum_variants = int(budget.get("maximum_variants", 0))
    if (
        not isinstance(names, list)
        or not names
        or len(names) != len(set(names))
        or not 1 <= maximum_variants <= min(8, int(evidence.get("maximum_variants", 8)))
    ):
        raise StrategyIntentError(
            "parameter budget is invalid or exceeds eight variants"
        )

    equations = candidate.get("equations", [])
    if any(
        not isinstance(item, dict)
        or item.get("verification")
        not in {"deterministically_verified", "independently_verified"}
        for item in equations
    ):
        raise StrategyIntentError(
            "equation-dependent intent lacks independent assurance"
        )
    authority = evidence.get("authority")
    if not isinstance(authority, dict) or any(
        authority.get(key) is not False
        for key in ("capital", "orders", "production_promotion", "self_approval")
    ):
        raise StrategyIntentError("strategy authoring authority is not research-only")

    strategy_name = _slug(str(candidate["candidate_key"]))
    return StrategyIntent(
        schema_version=SCHEMA_VERSION,
        intent_id=expected_question_digest,
        strategy_name=strategy_name,
        hypothesis_id=strategy_name.upper().replace("_", "-"),
        question=question,
        question_digest=expected_question_digest,
        predictor=str(candidate["predictor"]),
        target=str(candidate["target"]),
        horizon=str(candidate["horizon"]),
        predicted_direction=str(candidate["predicted_direction"]),
        causal_timing=str(candidate["causal_timing"]),
        null_hypothesis=str(candidate["null_hypothesis"]),
        mechanism=str(candidate["mechanism"]),
        rival_explanations=list(candidate.get("rival_explanations", [])),
        falsification_criteria=list(candidate.get("falsification_criteria", [])),
        parameter_names=[str(item) for item in names],
        maximum_variants=maximum_variants,
        venue=str(data.get("venue")),
        primary_instrument=str(data["instrument"]).upper(),
        instruments=instruments,
        source_timeframe="1m",
        research_timeframe=str(data["research_timeframe"]),
        resampling_policy=RESAMPLING_POLICY,
        window={"start": str(window["start"]), "end": str(window["end"])},
        dataset_bindings=bindings,
        representation_plan=plan,
        authority={
            key: False
            for key in ("capital", "orders", "production_promotion", "self_approval")
        },
        unresolved_authoring=[
            "signal_feature_and_gate_logic",
            "bounded_parameter_values",
            "native_evaluator_and_terminal_paths",
            "focused_runner_integration_tests",
        ],
        evidence_digest=digest(evidence),
    )


def _write_new_or_verify(path: Path, payload: bytes, marker: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if marker not in path.read_text(encoding="utf-8", errors="replace"):
            raise StrategyIntentError(
                f"existing scaffold is not bound to intent: {path}"
            )
        return "reused"
    path.write_bytes(payload)
    return "created"


def scaffold_strategy_package(
    repository: Path, intent: StrategyIntent
) -> dict[str, Any]:
    """Create deterministic package boundaries before any coding model runs."""
    root = repository.resolve(strict=True)
    document = intent.document()
    intent_digest = str(document["intent_digest"])
    marker = intent_digest
    name = intent.strategy_name
    class_name = "".join(part.title() for part in name.split("_")) + "Strategy"
    intent_path = root / "research" / "hypotheses" / "intents" / f"{name}.json"
    card_path = (
        root / "research" / "hypotheses" / "cards" / f"{intent.question_digest}.json"
    )
    yaml_path = root / "research" / "hypotheses" / f"{name}.yaml"
    strategy_path = root / "src" / "bt" / "strategy" / f"{name}.py"
    doc_path = root / "docs" / "hypotheses" / f"{name}.md"
    test_path = root / "tests" / "hypotheses" / f"test_{name}_scaffold.py"

    yaml_document = {
        "metadata": {
            "hypothesis_id": intent.hypothesis_id,
            "title": intent.question[:300],
            "description": intent.question,
            "research_layer": "ALPHA",
            "hypothesis_family": name,
            "version": "1.0.0",
        },
        "hypothesis_id": intent.hypothesis_id,
        "title": intent.question[:300],
        "description": intent.question,
        "research_layer": "ALPHA",
        "hypothesis_family": name,
        "version": "1.0.0",
        "status": "draft",
        "strategy_intent_digest": intent_digest,
        "immutable_contract": document,
        "representation_plan": intent.representation_plan,
        "parameter_grid": {
            key: ["__CODEX_REQUIRED__"] for key in intent.parameter_names
        },
        "entry": {
            "strategy": name,
            "signal_timeframe": intent.research_timeframe,
            "authority": "research_observation_only",
        },
        "exit": {"target_horizon": intent.horizon, "policy": "__CODEX_REQUIRED__"},
        "execution_semantics": {
            "signal_timeframe": intent.research_timeframe,
            "base_execution_timeframe": "1m",
            "base_data_frequency_expected": "1m",
            "exit_monitoring_timeframe": "1m",
            "hold_time_unit": "__CODEX_REQUIRED__",
            "atr_source_timeframe": intent.research_timeframe,
            "stop_model": "__CODEX_REQUIRED__",
            "stop_update_policy": "__CODEX_REQUIRED__",
            "tp_update_policy": "__CODEX_REQUIRED__",
            "adaptive_representation_plan_digest": digest(intent.representation_plan),
            "adaptive_representation_fields": [
                item["output_field"]
                for item in intent.representation_plan["transformations"]
            ],
        },
        "evaluation": {
            "required_tiers": ["Tier2B"],
            "outcome_retention": ["positive", "negative", "invalid", "failed"],
            "signal_estimand": {
                "target": intent.target,
                "horizon": intent.horizon,
                "direction": intent.predicted_direction,
            },
            "native_implementation": {
                "path": f"src/bt/strategy/{name}.py",
                "function": "evaluate_alpha_intent",
            },
        },
        "falsification_criteria": intent.falsification_criteria,
        "logging": {
            "required_fields": [
                "decision_trace",
                "decision_ts",
                "target_exit_ts",
                "outcome",
                "reason",
                "stop_price",
                "representation_plan_digest",
                "representation_output_fields",
                "representation_decision_ts",
            ]
        },
        "runtime_controls": {
            "enabled": True,
            "max_variants": intent.maximum_variants,
            "tags": ["alpha-003", "intent-scaffold", "classic-only"],
        },
        "truth_contract": TRUTH_CONTRACT,
        "authority": intent.authority,
    }
    representation_fields = [
        item["output_field"] for item in intent.representation_plan["transformations"]
    ]
    primary_binding = next(
        item
        for item in intent.dataset_bindings
        if intent.primary_instrument
        in [
            str(value).upper()
            for value in item.get("instruments", [item.get("instrument", "")])
        ]
    )
    card_document = {
        "schema_version": "hypothesis_card_v1",
        "card_id": f"{name.replace('_', '-')}-{intent.question_digest[:12]}",
        "program_id": "alpha-003",
        "version": 1,
        "status": "draft",
        "title": intent.question[:300],
        "claim": intent.question,
        "intuition": intent.mechanism,
        "market_mechanism": intent.mechanism,
        "engine_strategy_name": name,
        "engine_hypothesis_template": f"research/hypotheses/{name}.yaml",
        "strategy_intent_digest": intent_digest,
        "features": [
            {
                "id": field,
                "source": "adaptive_representation",
                "source_field": field,
                "transform": "identity",
                "lag": 0,
            }
            for field in representation_fields
        ],
        "gates": [
            {
                "left": representation_fields[0],
                "op": "__CODEX_REQUIRED__",
                "right_param": intent.parameter_names[0],
            }
        ],
        "entry": {
            "direction": "__CODEX_REQUIRED__",
            "timing": "strict_completed_bar_submit_next_1m_bar",
            "pyramiding": False,
            "flip": False,
        },
        "exit": {
            "type": "scientific_target",
            "target": intent.target,
            "horizon": intent.horizon,
            "policy": "__CODEX_REQUIRED__",
        },
        "sizing": {"mode": "research_observation", "notional_pct_equity": 0.0},
        "risk_controls": {
            "authority": "engine",
            "max_positions": 0,
            "no_capital_research": True,
        },
        "parameters": {key: ["__CODEX_REQUIRED__"] for key in intent.parameter_names},
        "data_requirements": ["research_panel"],
        "logging_requirements": yaml_document["logging"]["required_fields"],
        "evaluation": {
            "tiers": ["Tier2B"],
            "metrics": ["__CODEX_REQUIRED__"],
            "outcome_retention": ["positive", "negative", "invalid", "failed"],
            "native_implementation": yaml_document["evaluation"][
                "native_implementation"
            ],
        },
        "falsification_criteria": intent.falsification_criteria,
        "expected_failure_modes": intent.rival_explanations,
        "execution_semantics": {
            **TRUTH_CONTRACT,
            "signal_timeframe": intent.research_timeframe,
            "base_execution_timeframe": "1m",
            "base_data_frequency_expected": "1m",
            "adaptive_representation_plan_digest": digest(intent.representation_plan),
            "adaptive_representation_fields": representation_fields,
            "required_extra_columns": [
                *representation_fields,
                "representation_plan_digest",
                "representation_output_fields",
                "representation_decision_ts",
            ],
        },
        "source_citations": [],
        "field_provenance": {
            key: {
                "state": "recommended",
                "confidence": 1.0,
                "basis": "Frozen typed StrategyIntent",
            }
            for key in ("claim", "entry", "exit")
        },
        "dataset_binding": {
            "dataset_build_id": primary_binding["dataset_build_id"],
            "dataset_digest": primary_binding["dataset_digest"],
            "venue": intent.venue,
            "instrument": intent.primary_instrument,
            "timeframe": intent.source_timeframe,
        },
        "execution_window": intent.window,
        "immutable_contract": {
            "strategy_intent_digest": intent_digest,
            "tier": "Tier2B",
            "maximum_variants": intent.maximum_variants,
            "authority": intent.authority,
        },
        "research_question": intent.question,
    }
    strategy = f'''"""Native implementation for intent {intent_digest}."""\n\nfrom __future__ import annotations\n\nfrom typing import Any\n\nfrom bt.strategy import register_strategy\nfrom bt.strategy.base import Strategy\n\nSTRATEGY_INTENT_DIGEST = "{intent_digest}"\nQUESTION = {intent.question!r}\nINSTRUMENTS = {tuple(intent.instruments)!r}\n\n\ndef evaluate_alpha_intent(**context: Any) -> dict[str, Any]:\n    """Return retained positive, negative, invalid or failed native evidence."""\n    raise NotImplementedError("__CODEX_REQUIRED__: implement exact governed signal estimand")\n\n\n@register_strategy("{name}")\nclass {class_name}(Strategy):\n    def on_bars(self, ts, bars_by_symbol, tradeable, ctx):\n        raise NotImplementedError("__CODEX_REQUIRED__: implement causal classic-engine signals")\n'''
    documentation = f"""# {intent.question}\n\nIntent digest: `{intent_digest}`\n\n## Frozen scientific contract\n\n- Predictor: {intent.predictor}\n- Target: {intent.target}\n- Horizon: {intent.horizon}\n- Direction: {intent.predicted_direction}\n- Research timeframe: {intent.research_timeframe}\n- Instruments: {", ".join(intent.instruments)}\n- Authority: research only; no capital, orders, promotion or self-approval\n\n## Authoring boundary\n\nCodex must implement only the unresolved signal, bounded parameter values, native evaluator and focused tests. Dataset identities, representation, clocks, question and authority are immutable.\n"""
    test = f'''from pathlib import Path\n\n\ndef test_generated_package_has_no_unresolved_placeholders():\n    root = Path(__file__).resolve().parents[2]\n    paths = [\n        root / "research/hypotheses/cards/{intent.question_digest}.json",\n        root / "research/hypotheses/{name}.yaml",\n        root / "src/bt/strategy/{name}.py",\n    ]\n    for path in paths:\n        assert "__CODEX_REQUIRED__" not in path.read_text(encoding="utf-8")\n\n\ndef test_generated_package_retains_intent_digest():\n    root = Path(__file__).resolve().parents[2]\n    expected = "{intent_digest}"\n    assert expected in (root / "research/hypotheses/cards/{intent.question_digest}.json").read_text(encoding="utf-8")\n    assert expected in (root / "research/hypotheses/{name}.yaml").read_text(encoding="utf-8")\n    assert expected in (root / "src/bt/strategy/{name}.py").read_text(encoding="utf-8")\n'''
    statuses = {
        str(intent_path.relative_to(root)): _write_new_or_verify(
            intent_path,
            json.dumps(document, indent=2, sort_keys=True).encode() + b"\n",
            marker,
        ),
        str(card_path.relative_to(root)): _write_new_or_verify(
            card_path,
            json.dumps(card_document, indent=2, sort_keys=True).encode() + b"\n",
            marker,
        ),
        str(yaml_path.relative_to(root)): _write_new_or_verify(
            yaml_path, yaml.safe_dump(yaml_document, sort_keys=False).encode(), marker
        ),
        str(strategy_path.relative_to(root)): _write_new_or_verify(
            strategy_path, strategy.encode(), marker
        ),
        str(doc_path.relative_to(root)): _write_new_or_verify(
            doc_path, documentation.encode(), marker
        ),
        str(test_path.relative_to(root)): _write_new_or_verify(
            test_path, test.encode(), marker
        ),
    }
    return {
        "schema_version": SCAFFOLD_VERSION,
        "intent_digest": intent_digest,
        "strategy_name": name,
        "paths": statuses,
        "unresolved_authoring": intent.unresolved_authoring,
        "authority": intent.authority,
        "scaffold_digest": digest({"intent": document, "paths": statuses}),
    }
