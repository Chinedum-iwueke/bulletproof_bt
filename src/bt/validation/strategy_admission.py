"""Fail-closed admission checks for generated hypothesis strategy packages."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import inspect
import json
from pathlib import Path
from typing import Any

import yaml

from bt.hypotheses.contract import HypothesisContract
from bt.hypotheses.logging import REQUIRED_LOG_FIELDS
from bt.strategy import STRATEGY_REGISTRY


TRUTH_CONTRACT_VERSION = "1.0"
FORBIDDEN_CAUSAL_PATTERNS = (
    ".shift(-",
    ".bfill(",
    ".interpolate(",
    'direction="forward"',
    "direction='forward'",
)


@dataclass(frozen=True)
class AdmissionIssue:
    check: str
    message: str
    severity: str = "error"


@dataclass
class StrategyAdmissionReport:
    hypothesis_path: str
    strategy_name: str | None
    strategy_path: str | None
    hypothesis_sha256: str
    strategy_sha256: str | None
    status: str
    issues: list[AdmissionIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["contract_version"] = TRUTH_CONTRACT_VERSION
        payload["hard_failures"] = sum(issue.severity == "error" for issue in self.issues)
        payload["warnings"] = sum(issue.severity == "warning" for issue in self.issues)
        return payload


def _digest(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _issue(issues: list[AdmissionIssue], check: str, message: str, severity: str = "error") -> None:
    issues.append(AdmissionIssue(check=check, message=message, severity=severity))


def validate_hypothesis_admission(
    hypothesis_path: str | Path,
    *,
    require_truth_contract: bool = True,
) -> StrategyAdmissionReport:
    path = Path(hypothesis_path)
    issues: list[AdmissionIssue] = []
    strategy_name: str | None = None
    strategy_path: Path | None = None
    if not path.exists():
        _issue(issues, "hypothesis_exists", f"Hypothesis YAML does not exist: {path}")
        return StrategyAdmissionReport(str(path), None, None, "", None, "FAIL", issues)

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise TypeError("top-level YAML must be a mapping")
        contract = HypothesisContract.from_dict(raw)
    except Exception as exc:
        _issue(issues, "hypothesis_contract", f"Hypothesis contract is invalid: {exc}")
        return StrategyAdmissionReport(str(path), None, None, _digest(path), None, "FAIL", issues)

    truth = raw.get("truth_contract") if isinstance(raw.get("truth_contract"), dict) else None
    if truth is None:
        severity = "error" if require_truth_contract else "warning"
        _issue(issues, "truth_contract", "Missing mandatory truth_contract block", severity)
    else:
        expected = {
            "version": TRUTH_CONTRACT_VERSION,
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
            "research_memory_requires_certification": True,
        }
        for key, value in expected.items():
            if truth.get(key) != value:
                _issue(issues, f"truth_contract.{key}", f"Expected {value!r}, got {truth.get(key)!r}")

    sem = contract.schema.execution_semantics
    exact_semantics = {
        "base_data_frequency_expected": "1m",
        "base_execution_timeframe": "1m",
        "exit_monitoring_timeframe": "1m",
    }
    for key, expected in exact_semantics.items():
        if str(sem.get(key, "")).lower() != expected:
            _issue(issues, f"execution_semantics.{key}", f"Must be {expected!r}")
    for key in (
        "signal_timeframe",
        "stop_model",
        "stop_update_policy",
        "tp_update_policy",
        "hold_time_unit",
        "atr_source_timeframe",
    ):
        if not sem.get(key):
            _issue(issues, f"execution_semantics.{key}", "Required execution semantic is missing")
    if sem.get("risk_accounting") not in (None, "engine_canonical_R"):
        _issue(issues, "execution_semantics.risk_accounting", "Must be engine_canonical_R when declared")

    missing_log_fields = sorted(set(REQUIRED_LOG_FIELDS) - set(contract.logging_fields()))
    if missing_log_fields:
        _issue(issues, "logging.required_fields", f"Missing canonical fields: {missing_log_fields}")

    strategy_name = str(contract.schema.entry.get("strategy", "")).strip() or None
    if strategy_name is None:
        _issue(issues, "strategy_reference", "entry.strategy is required")
    elif strategy_name not in STRATEGY_REGISTRY:
        _issue(issues, "strategy_registration", f"Strategy is not registered: {strategy_name}")
    else:
        source = inspect.getsourcefile(STRATEGY_REGISTRY[strategy_name])
        strategy_path = Path(source) if source else None
        if strategy_path is None or not strategy_path.exists():
            _issue(issues, "strategy_source", f"Unable to resolve source for {strategy_name}")
        else:
            text = strategy_path.read_text(encoding="utf-8")
            required_patterns = {
                "strategy_on_bars": "on_bars",
                "volatile_membership_gate": "tradeable",
                "decision_trace": "decision_trace",
                "explicit_stop": "stop_price",
                "close_only_exit": "close_only",
                "signal_metadata": "metadata=",
            }
            for check, pattern in required_patterns.items():
                if pattern not in text:
                    _issue(issues, check, f"Strategy source does not contain required pattern {pattern!r}")
            for pattern in FORBIDDEN_CAUSAL_PATTERNS:
                if pattern in text:
                    _issue(issues, "forbidden_causal_pattern", f"Strategy source contains {pattern!r}")

    variants = contract.materialize_grid()
    if not variants:
        _issue(issues, "parameter_grid", "Parameter grid materializes zero valid variants")
    elif len({row["config_hash"] for row in variants}) != len(variants):
        _issue(issues, "parameter_grid_hash", "Materialized variants do not have unique config hashes")

    hard = any(issue.severity == "error" for issue in issues)
    return StrategyAdmissionReport(
        hypothesis_path=str(path),
        strategy_name=strategy_name,
        strategy_path=str(strategy_path) if strategy_path else None,
        hypothesis_sha256=_digest(path),
        strategy_sha256=_digest(strategy_path) if strategy_path and strategy_path.exists() else None,
        status="FAIL" if hard else "PASS",
        issues=issues,
    )


def write_strategy_admission_report(report: StrategyAdmissionReport, path: str | Path) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    return output
