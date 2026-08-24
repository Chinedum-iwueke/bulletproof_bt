"""Versioned, digest-bound market and execution model contracts."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any, Literal


MODEL_BUNDLE_SCHEMA_VERSION = "market-model-bundle-v1.0.0"
MODEL_CARD_SCHEMA_VERSION = "market-model-card-v1.0.0"
ModelKind = Literal[
    "fill",
    "queue_latency",
    "fee",
    "rebate",
    "spread",
    "slippage_impact",
    "funding",
    "borrow",
    "capacity",
]
SupportStatus = Literal["supported", "unsupported", "unavailable"]


class MarketModelError(ValueError):
    """A market-model declaration is incomplete or unsafe to use."""


def _canonical(document: Any) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def _digest(document: Any) -> str:
    return hashlib.sha256(_canonical(document)).hexdigest()


def _is_digest(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _validate_number(value: Any, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise MarketModelError(f"{field} must be a finite number")


@dataclass(frozen=True)
class CalibrationProvenance:
    source: str
    dataset_digest: str | None
    sample_start: str | None
    sample_end: str | None
    method: str
    fit_diagnostics: dict[str, float]
    holdout_diagnostics: dict[str, float]

    def validate(self, *, supported: bool) -> None:
        if not self.source.strip() or not self.method.strip():
            raise MarketModelError("calibration source and method are required")
        if self.dataset_digest is not None and not _is_digest(self.dataset_digest):
            raise MarketModelError("calibration dataset_digest must be lowercase sha256")
        if (self.sample_start is None) != (self.sample_end is None):
            raise MarketModelError("calibration sample_start and sample_end must be declared together")
        if supported and self.source == "empirical" and self.dataset_digest is None:
            raise MarketModelError("empirical models require a calibration dataset digest")
        for group_name, group in (
            ("fit_diagnostics", self.fit_diagnostics),
            ("holdout_diagnostics", self.holdout_diagnostics),
        ):
            for key, value in group.items():
                _validate_number(value, field=f"calibration.{group_name}.{key}")


@dataclass(frozen=True)
class MarketModelCard:
    model_id: str
    version: str
    kind: ModelKind
    support_status: SupportStatus
    implementation: str | None
    applicability: dict[str, tuple[str, ...]]
    timestamp_semantics: str
    parameters: dict[str, float | int | str | bool | None]
    uncertainty: dict[str, float]
    stress_ranges: dict[str, tuple[float, float]]
    calibration: CalibrationProvenance
    fallback: str
    incompatibilities: tuple[str, ...] = ()
    unsupported_reason: str | None = None

    def validate(self) -> None:
        if not self.model_id.strip() or not self.version.strip():
            raise MarketModelError("model_id and version are required")
        if not self.timestamp_semantics.strip() or not self.fallback.strip():
            raise MarketModelError("timestamp_semantics and fallback are required")
        if self.support_status == "supported" and not self.implementation:
            raise MarketModelError(f"supported model {self.model_id} requires an implementation")
        if self.support_status != "supported" and not self.unsupported_reason:
            raise MarketModelError(f"{self.support_status} model {self.model_id} requires a reason")
        for key, value in self.parameters.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                _validate_number(value, field=f"parameters.{key}")
        for key, value in self.uncertainty.items():
            _validate_number(value, field=f"uncertainty.{key}")
            if float(value) < 0:
                raise MarketModelError(f"uncertainty.{key} must be >= 0")
        for key, bounds in self.stress_ranges.items():
            if len(bounds) != 2:
                raise MarketModelError(f"stress range {key} must have lower and upper bounds")
            lower, upper = bounds
            _validate_number(lower, field=f"stress_ranges.{key}.lower")
            _validate_number(upper, field=f"stress_ranges.{key}.upper")
            if lower > upper:
                raise MarketModelError(f"stress range {key} is reversed")
        self.calibration.validate(supported=self.support_status == "supported")

    def document(self) -> dict[str, Any]:
        self.validate()
        document = {"schema_version": MODEL_CARD_SCHEMA_VERSION, **asdict(self)}
        document["card_digest"] = _digest(document)
        return document


@dataclass(frozen=True)
class MarketModelBundle:
    name: str
    version: str
    models: tuple[MarketModelCard, ...]

    def document(self) -> dict[str, Any]:
        if not self.name.strip() or not self.version.strip():
            raise MarketModelError("bundle name and version are required")
        cards = [card.document() for card in self.models]
        identities = [(card["kind"], card["model_id"], card["version"]) for card in cards]
        if len(identities) != len(set(identities)):
            raise MarketModelError("market-model bundle contains duplicate identities")
        document = {
            "schema_version": MODEL_BUNDLE_SCHEMA_VERSION,
            "name": self.name,
            "version": self.version,
            "models": sorted(cards, key=lambda card: (card["kind"], card["model_id"], card["version"])),
        }
        document["bundle_digest"] = _digest(document)
        return document

    @property
    def digest(self) -> str:
        return str(self.document()["bundle_digest"])

    def require(self, kind: ModelKind) -> MarketModelCard:
        matches = [model for model in self.models if model.kind == kind]
        if len(matches) != 1:
            raise MarketModelError(f"exactly one {kind} model must be declared")
        model = matches[0]
        model.validate()
        if model.support_status != "supported":
            raise MarketModelError(
                f"{kind} model is {model.support_status}: {model.unsupported_reason}; fallback={model.fallback}"
            )
        return model


def assert_pessimistic_cost_order(*, baseline_cost: float, stressed_cost: float) -> None:
    """Fail if a declared pessimistic execution-cost stress improves costs."""
    _validate_number(baseline_cost, field="baseline_cost")
    _validate_number(stressed_cost, field="stressed_cost")
    if baseline_cost < 0 or stressed_cost < 0:
        raise MarketModelError("execution costs must be non-negative")
    if stressed_cost + 1e-12 < baseline_cost:
        raise MarketModelError("pessimistic stress cannot reduce execution costs")


def validate_model_bundle_document(document: dict[str, Any]) -> None:
    """Validate a serialized bundle without trusting its supplied digest."""
    if document.get("schema_version") != MODEL_BUNDLE_SCHEMA_VERSION:
        raise MarketModelError("unsupported market-model bundle schema version")
    supplied = document.get("bundle_digest")
    expected = _digest({key: value for key, value in document.items() if key != "bundle_digest"})
    if supplied != expected:
        raise MarketModelError("market-model bundle digest mismatch")
    cards = document.get("models")
    if not isinstance(cards, list) or not cards:
        raise MarketModelError("market-model bundle must contain model cards")
    identities: list[tuple[Any, Any, Any]] = []
    for card in cards:
        if not isinstance(card, dict) or card.get("schema_version") != MODEL_CARD_SCHEMA_VERSION:
            raise MarketModelError("invalid market-model card schema")
        card_digest = card.get("card_digest")
        expected_card = _digest({key: value for key, value in card.items() if key != "card_digest"})
        if card_digest != expected_card:
            raise MarketModelError("market-model card digest mismatch")
        identities.append((card.get("kind"), card.get("model_id"), card.get("version")))
    if len(identities) != len(set(identities)):
        raise MarketModelError("market-model bundle contains duplicate identities")


def declared_classic_bundle(*, profile: str, parameters: dict[str, float | int]) -> MarketModelBundle:
    """Bind the existing classic mechanics without claiming empirical calibration."""
    provenance = CalibrationProvenance(
        source="declared-policy",
        dataset_digest=None,
        sample_start=None,
        sample_end=None,
        method="founder-approved deterministic execution profile",
        fit_diagnostics={},
        holdout_diagnostics={},
    )

    def supported(kind: ModelKind, model_id: str, implementation: str, selected: dict[str, Any], stress: dict[str, tuple[float, float]]) -> MarketModelCard:
        return MarketModelCard(
            model_id=model_id,
            version="1.0.0",
            kind=kind,
            support_status="supported",
            implementation=implementation,
            applicability={"profiles": (profile,), "products": ("declared-by-run",)},
            timestamp_semantics="decision-known parameters applied at causal fill event",
            parameters=selected,
            uncertainty={"calibration_error": 0.0},
            stress_ranges=stress,
            calibration=provenance,
            fallback="fail-closed",
        )

    def unsupported(kind: ModelKind, reason: str) -> MarketModelCard:
        return MarketModelCard(
            model_id=f"classic-{kind}-unsupported",
            version="1.0.0",
            kind=kind,
            support_status="unsupported",
            implementation=None,
            applicability={"profiles": (profile,), "products": ("all",)},
            timestamp_semantics="not applied",
            parameters={},
            uncertainty={},
            stress_ranges={},
            calibration=provenance,
            fallback="reject any run requiring this cost",
            unsupported_reason=reason,
        )

    fee_bps = float(parameters.get("taker_fee_bps", 0.0))
    slippage_bps = float(parameters.get("slippage_bps", 0.0))
    spread_bps = float(parameters.get("spread_bps", 0.0))
    delay_bars = int(parameters.get("delay_bars", 0))
    models = (
        supported("fill", "classic-market-fill", "bt.execution.execution_model.ExecutionModel", {"delay_bars": delay_bars, "partial_fill": False}, {"delay_bars": (delay_bars, delay_bars + 5)}),
        supported("queue_latency", "classic-bar-delay", "bt.execution.execution_model.ExecutionModel", {"delay_bars": delay_bars}, {"delay_bars": (delay_bars, delay_bars + 5)}),
        supported("fee", "classic-notional-fee", "bt.execution.fees.FeeModel", {"taker_fee_bps": fee_bps}, {"taker_fee_bps": (fee_bps, max(fee_bps * 3.0, fee_bps))}),
        supported("spread", "classic-fixed-spread", "bt.execution.spread.apply_instrument_spread", {"spread_bps": spread_bps}, {"spread_bps": (spread_bps, max(spread_bps * 3.0, spread_bps))}),
        supported("slippage_impact", "classic-volume-volatility-impact", "bt.execution.slippage.SlippageModel", {"fixed_bps": slippage_bps}, {"fixed_bps": (slippage_bps, max(slippage_bps * 3.0, slippage_bps))}),
        unsupported("rebate", "maker rebates are not credited by the classic execution engine"),
        unsupported("funding", "funding observations are features but funding cashflows are not charged"),
        unsupported("borrow", "borrow availability and borrow charges are not modeled"),
        unsupported("capacity", "capacity is diagnostic-only and does not constrain fills"),
    )
    return MarketModelBundle(name=f"classic-{profile}", version="1.0.0", models=models)
