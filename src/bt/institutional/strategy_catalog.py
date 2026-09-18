from __future__ import annotations

from math import prod
import hashlib
from pathlib import Path
from typing import Any

import yaml

from bt.hypotheses.contract import HypothesisContract
from bt.contracts.research_specs_v2 import EXACT_TRUTH
from bt.institutional.receipt import digest
from bt.strategy import STRATEGY_REGISTRY


_RESEARCH_CONTRACT_FIELDS = (
    "required_indicators",
    "indicator_defaults",
    "gates",
    "parameter_grid",
    "entry",
    "exit",
    "execution_semantics",
    "sizing",
    "risk_controls",
    "data_assumptions",
    "evaluation",
    "falsification_criteria",
    "truth_contract",
    "expected_failure_modes",
)


def _research_contract(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: raw[key] for key in _RESEARCH_CONTRACT_FIELDS if key in raw}


def build_strategy_capability_catalog(
    repository_root: Path, *, source_commit: str
) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    hypothesis_root = (root / "research" / "hypotheses").resolve(strict=True)
    capabilities = []
    for path in sorted(hypothesis_root.glob("*.yaml")):
        contract = HypothesisContract.from_yaml(path)
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        parameters = contract.schema.parameter_grid
        variant_count = prod(len(values) for values in parameters.values())
        strategy = str(contract.schema.entry["strategy"])
        if strategy not in STRATEGY_REGISTRY:
            continue
        relative = path.relative_to(root).as_posix()
        logging_requirements = list(contract.schema.logging.required_fields)
        explicit_logging = raw.get("logging", {}).get("required_fields", [])
        semantic_blockers = [
            key
            for key, expected in EXACT_TRUTH.items()
            if raw.get("truth_contract", {}).get(key) != expected
        ]
        fixture_only = (
            contract.schema.metadata.hypothesis_family == "pipeline_smoke"
            or "smoke" in contract.schema.metadata.hypothesis_id.lower()
            or "fixture" in contract.schema.metadata.hypothesis_id.lower()
        )
        reuse_blockers = [
            *[f"truth_semantics:{key}" for key in semantic_blockers],
            *([] if explicit_logging else ["logging_contract_missing"]),
            *([] if 1 <= variant_count <= 8 else ["variant_budget_exceeded"]),
            *([] if not fixture_only else ["non_research_fixture"]),
        ]
        research_contract = _research_contract(raw)
        capabilities.append(
            {
                "hypothesis_id": contract.schema.metadata.hypothesis_id,
                "title": contract.schema.metadata.title,
                "description": contract.schema.metadata.description,
                "hypothesis_family": contract.schema.metadata.hypothesis_family,
                "strategy": strategy,
                "input_mode": "single_instrument",
                "maximum_instruments": 1,
                "signal_timeframes": sorted(
                    {
                        str(values)
                        for key, options in parameters.items()
                        if key in {"signal_timeframe", "timeframe"}
                        for values in options
                    }
                    or {
                        str(
                            contract.schema.execution_semantics.get(
                                "signal_timeframe", "1m"
                            )
                        )
                    }
                ),
                "variant_count": variant_count,
                "logging_requirements": logging_requirements,
                "reuse_blockers": reuse_blockers,
                "bounded_weekly_reuse_eligible": not reuse_blockers,
                "contract_path": relative,
                "contract_digest": hashlib.sha256(path.read_bytes()).hexdigest(),
                "research_contract": research_contract,
                "research_contract_digest": digest(research_contract),
            }
        )
    core = {
        "schema_version": "alpha-strategy-capability-catalog-v1.1.0",
        "source_commit": source_commit,
        "capabilities": capabilities,
        "capital_or_order_authority": False,
        "claim_boundary": (
            "Catalog membership proves an existing native hypothesis contract and registered "
            "strategy implementation at the exact source commit. It does not establish alpha."
        ),
    }
    return {**core, "catalog_digest": digest(core)}
