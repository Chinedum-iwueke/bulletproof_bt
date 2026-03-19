"""Hypothesis contract loader, validator, and materializer."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import yaml

from bt.hypotheses.exceptions import InvalidHypothesisSchemaError, MissingIndicatorDependencyError
from bt.hypotheses.logging import REQUIRED_LOG_FIELDS
from bt.hypotheses.materialize import canonical_json_hash, materialize_grid
from bt.hypotheses.schema import EvaluationSpec, HypothesisSchema, LoggingSpec, Metadata, RuntimeControls
from bt.indicators.registry import INDICATOR_REGISTRY


class HypothesisContract:
    def __init__(self, schema: HypothesisSchema) -> None:
        self.schema = schema

    @classmethod
    def from_yaml(cls, path: str | Path) -> "HypothesisContract":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HypothesisContract":
        md = Metadata(
            hypothesis_id=str(data["hypothesis_id"]),
            title=str(data["title"]),
            description=str(data.get("description", "")),
            research_layer=str(data["research_layer"]),
            hypothesis_family=str(data["hypothesis_family"]),
            version=str(data["version"]),
            author=str(data.get("author", "")),
            created_at=str(data.get("created_at", "")),
        )
        required_indicators = tuple(str(v) for v in data.get("required_indicators", []))
        grid_raw = data.get("parameter_grid", {})
        parameter_grid = {str(k): tuple(v) for k, v in grid_raw.items()}
        evaluation_raw = data.get("evaluation", {})
        logging_raw = data.get("logging", {})
        runtime_raw = data.get("runtime_controls", {})
        execution_semantics_raw = data.get("execution_semantics", {})
        schema = HypothesisSchema(
            metadata=md,
            required_indicators=required_indicators,
            indicator_defaults=dict(data.get("indicator_defaults", {})),
            parameter_grid=parameter_grid,
            gates=tuple(data.get("gates", [])),
            entry=dict(data.get("entry", {})),
            exit=dict(data.get("exit", {})),
            execution_semantics=dict(execution_semantics_raw),
            evaluation=EvaluationSpec(required_tiers=tuple(evaluation_raw.get("required_tiers", ["Tier2", "Tier3"]))),
            logging=LoggingSpec(
                schema_version=str(logging_raw.get("schema_version", "1.0")),
                required_fields=tuple(logging_raw.get("required_fields", REQUIRED_LOG_FIELDS)),
            ),
            runtime=RuntimeControls(
                enabled=bool(runtime_raw.get("enabled", True)),
                max_variants=runtime_raw.get("max_variants"),
                tags=tuple(runtime_raw.get("tags", [])),
                notes=str(runtime_raw.get("notes", "")),
            ),
        )
        contract = cls(schema)
        contract.validate()
        return contract


    def _validate_two_clock_semantics(self) -> None:
        sem = self.schema.execution_semantics
        if not sem:
            return
        required = (
            "signal_timeframe",
            "base_execution_timeframe",
            "base_data_frequency_expected",
            "stop_model",
            "stop_update_policy",
            "tp_update_policy",
            "hold_time_unit",
            "exit_monitoring_timeframe",
            "atr_source_timeframe",
        )
        missing = [key for key in required if key not in sem]
        if missing:
            raise InvalidHypothesisSchemaError(f"execution_semantics missing required keys: {missing}")

    def validate(self) -> None:
        if not self.schema.metadata.hypothesis_id:
            raise InvalidHypothesisSchemaError("hypothesis_id is required")
        if not self.schema.parameter_grid:
            raise InvalidHypothesisSchemaError("parameter_grid must be non-empty")
        unknown = [name for name in self.schema.required_indicators if name.lower() not in INDICATOR_REGISTRY]
        if unknown:
            raise MissingIndicatorDependencyError(f"unregistered required indicators: {unknown}")
        self._validate_two_clock_semantics()

    def materialize_grid(self) -> list[dict[str, Any]]:
        base = materialize_grid(self.schema.parameter_grid, max_variants=self.schema.runtime.max_variants)
        payload: list[dict[str, Any]] = []
        for variant in base:
            params = variant["params"]
            if not self._is_valid_variant(params):
                continue
            payload.append({
                "hypothesis_id": self.schema.metadata.hypothesis_id,
                "title": self.schema.metadata.title,
                "contract_version": self.schema.metadata.version,
                "grid_id": variant["grid_id"],
                "config_hash": variant["config_hash"],
                "params": params,
                "required_tiers": list(self.required_tiers()),
                "required_indicators": list(self.required_indicators()),
            })
        return payload

    def _is_valid_variant(self, params: dict[str, Any]) -> bool:
        if "z_reentry" in params and "z_ext" in params:
            try:
                if float(params["z_reentry"]) >= float(params["z_ext"]):
                    return False
            except (TypeError, ValueError):
                return False

        mode = params.get("trail_activation_mode")
        if mode is None:
            return True

        bars_values = self.schema.parameter_grid.get("trail_activate_after_bars")
        profit_values = self.schema.parameter_grid.get("trail_activate_after_profit_r")
        if not bars_values or not profit_values:
            return True

        bars_default = bars_values[0]
        profit_default = profit_values[0]
        if str(mode) == "bars":
            return params.get("trail_activate_after_profit_r") == profit_default
        if str(mode) == "profit_r":
            return params.get("trail_activate_after_bars") == bars_default
        return True

    def required_tiers(self) -> tuple[str, ...]:
        return self.schema.evaluation.required_tiers or ("Tier2", "Tier3")

    def required_indicators(self) -> tuple[str, ...]:
        return self.schema.required_indicators

    def logging_fields(self) -> tuple[str, ...]:
        return self.schema.logging.required_fields

    def fingerprint_variant(self, variant_params: dict[str, Any]) -> str:
        return canonical_json_hash(variant_params)

    def to_run_specs(self) -> list[dict[str, Any]]:
        return self.materialize_grid()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self.schema)
