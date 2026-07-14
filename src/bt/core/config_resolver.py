"""Resolve and validate runtime configuration into a single canonical shape."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import copy

from bt.benchmarks.config import BenchmarkConfigError, parse_benchmark_config
from bt.core.errors import ConfigError
from bt.execution.intrabar import parse_intrabar_spec
from bt.execution.profile import resolve_execution_profile


@dataclass(frozen=True)
class ResolvedConfig:
    raw: dict[str, Any]
    resolved: dict[str, Any]



_LEGACY_STOP_RESOLUTION_WARNING = (
    "risk.stop_resolution=allow_legacy_proxy is deprecated; "
    "use risk.stop_resolution=safe and risk.allow_legacy_proxy=true"
)


def _normalize_symbol_list(value: Any, *, key_path: str) -> list[str]:
    if not isinstance(value, list):
        raise ConfigError(
            f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})"
        )

    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ConfigError(
                f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})"
            )
        symbol = item.strip()
        if not symbol:
            continue
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)

    if not normalized:
        raise ConfigError(
            f"Invalid config: {key_path} must be a non-empty list of strings (got: {value!r})"
        )
    return normalized


def _resolve_data_symbols_alias(resolved: dict[str, Any]) -> None:
    data_cfg = _ensure_mapping(resolved.get("data"), name="data")
    subset = data_cfg.get("symbols_subset")
    symbols = data_cfg.get("symbols")

    if symbols is None and subset is None:
        resolved["data"] = data_cfg
        return

    normalized_subset = None if subset is None else _normalize_symbol_list(subset, key_path="data.symbols_subset")
    normalized_symbols = None if symbols is None else _normalize_symbol_list(symbols, key_path="data.symbols")

    if normalized_symbols is not None and normalized_subset is None:
        data_cfg["symbols_subset"] = normalized_symbols
    elif normalized_symbols is not None and normalized_subset is not None and normalized_symbols != normalized_subset:
        raise ConfigError(
            "Config conflict: data.symbols and data.symbols_subset both set but differ. "
            f"Use only one. data.symbols={symbols!r} data.symbols_subset={subset!r}"
        )
    elif normalized_subset is not None:
        data_cfg["symbols_subset"] = normalized_subset

    resolved["data"] = data_cfg


def _ensure_mapping(value: Any, *, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{name} must be a mapping when provided")
    return value




def _resolve_instrument(resolved: dict[str, Any]) -> None:
    instrument_raw = resolved.get("instrument")
    if instrument_raw is None:
        return
    instrument_cfg = _ensure_mapping(instrument_raw, name="instrument")
    instrument_cfg.setdefault("type", "crypto")

    allowed_types = {"crypto", "forex", "equity", "futures"}
    instrument_type = instrument_cfg.get("type")
    if instrument_type not in allowed_types:
        raise ConfigError(
            "instrument.type must be one of "
            f"{sorted(allowed_types)} (got: {instrument_type!r})"
        )

    symbol = instrument_cfg.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ConfigError(f"instrument.symbol must be a non-empty string (got: {symbol!r})")

    for key in ("tick_size", "contract_size", "pip_size", "pip_value"):
        raw = instrument_cfg.get(key)
        if raw is None:
            continue
        try:
            parsed = float(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"instrument.{key} must be > 0 (got: {raw!r})") from exc
        if parsed <= 0:
            raise ConfigError(f"instrument.{key} must be > 0 (got: {raw!r})")
        instrument_cfg[key] = parsed

    resolved["instrument"] = instrument_cfg


def _resolve_risk_value(
    *,
    resolved: dict[str, Any],
    top_key: str,
    nested_key: str,
    default: Any,
) -> None:
    risk_cfg = _ensure_mapping(resolved.get("risk"), name="risk")
    top_present = top_key in resolved
    nested_present = nested_key in risk_cfg

    if top_present and nested_present and resolved[top_key] != risk_cfg[nested_key]:
        raise ConfigError(
            f"Conflicting config values for '{top_key}' ({resolved[top_key]!r}) "
            f"and 'risk.{nested_key}' ({risk_cfg[nested_key]!r}). "
            "Define only one or make them equal."
        )

    if nested_present:
        canonical_value = risk_cfg[nested_key]
    elif top_present:
        canonical_value = resolved[top_key]
    else:
        canonical_value = default

    risk_cfg[nested_key] = canonical_value
    resolved["risk"] = risk_cfg


def _resolve_r_per_trade_alias(resolved: dict[str, Any]) -> None:
    """Normalize legacy risk aliases to canonical ``risk.r_per_trade``.

    ``risk.risk_per_trade_pct`` and top-level ``risk_per_trade_pct`` are treated
    as input aliases only. They are never injected by default.
    """
    risk_cfg = _ensure_mapping(resolved.get("risk"), name="risk")
    canonical_present = "r_per_trade" in risk_cfg
    nested_legacy_present = "risk_per_trade_pct" in risk_cfg
    top_legacy_present = "risk_per_trade_pct" in resolved

    canonical_value = risk_cfg.get("r_per_trade")
    nested_legacy_value = risk_cfg.get("risk_per_trade_pct")
    top_legacy_value = resolved.get("risk_per_trade_pct")

    if canonical_present and nested_legacy_present and canonical_value != nested_legacy_value:
        raise ConfigError(
            "Conflicting config values for 'risk.r_per_trade' "
            f"({canonical_value!r}) and 'risk.risk_per_trade_pct' ({nested_legacy_value!r})."
        )

    if canonical_present and top_legacy_present and canonical_value != top_legacy_value:
        raise ConfigError(
            "Conflicting config values for 'risk.r_per_trade' "
            f"({canonical_value!r}) and 'risk_per_trade_pct' ({top_legacy_value!r})."
        )

    if nested_legacy_present and top_legacy_present and nested_legacy_value != top_legacy_value:
        raise ConfigError(
            "Conflicting config values for 'risk.risk_per_trade_pct' "
            f"({nested_legacy_value!r}) and 'risk_per_trade_pct' ({top_legacy_value!r})."
        )

    if not canonical_present:
        if nested_legacy_present:
            risk_cfg["r_per_trade"] = nested_legacy_value
        elif top_legacy_present:
            risk_cfg["r_per_trade"] = top_legacy_value

    resolved["risk"] = risk_cfg


def _resolve_sizing_block(resolved: dict[str, Any]) -> None:
    sizing_cfg = resolved.get("sizing")
    if sizing_cfg is None:
        return
    if not isinstance(sizing_cfg, dict):
        raise ConfigError("Invalid sizing: expected mapping")
    risk_cfg = _ensure_mapping(resolved.get("risk"), name="risk")

    mapping = {
        "mode": "mode",
        "r_per_trade": "r_per_trade",
        "notional_pct_equity": "notional_pct_equity",
        "cap_policy": "cap_policy",
        "min_risk_utilization_pct": "min_risk_utilization_pct",
        "report_under_risked_trades": "report_under_risked_trades",
    }
    for sizing_key, risk_key in mapping.items():
        if sizing_key not in sizing_cfg:
            continue
        sizing_value = sizing_cfg[sizing_key]
        if risk_key in risk_cfg and risk_cfg[risk_key] != sizing_value:
            raise ConfigError(
                f"Conflicting config values for 'sizing.{sizing_key}' ({sizing_value!r}) "
                f"and 'risk.{risk_key}' ({risk_cfg[risk_key]!r}). Define only one or make them equal."
            )
        risk_cfg[risk_key] = sizing_value

    resolved["risk"] = risk_cfg


def resolve_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize config into one authoritative shape.
    - Enforce precedence rules.
    - Reject contradictions (don't silently pick).
    - Return a deep-copied resolved dict to be used by runners/engine wiring.
    """
    resolved = copy.deepcopy(cfg)
    if not isinstance(resolved, dict):
        raise ConfigError("Config root must be a mapping")

    resolved.setdefault("signal_delay_bars", 1)
    resolved.setdefault("initial_cash", 100000.0)
    resolved.setdefault("model", "fixed_bps")
    resolved.setdefault("fixed_bps", 5.0)

    outputs_cfg = _ensure_mapping(resolved.get("outputs"), name="outputs")
    outputs_cfg.setdefault("root_dir", "outputs/runs")
    outputs_cfg.setdefault("jsonl", True)
    resolved["outputs"] = outputs_cfg

    data_cfg = _ensure_mapping(resolved.get("data"), name="data")
    data_cfg.setdefault("mode", "streaming")
    data_cfg.setdefault("symbols_subset", None)
    data_cfg.setdefault("chunksize", 50000)
    resolved["data"] = data_cfg
    _resolve_data_symbols_alias(resolved)
    _resolve_instrument(resolved)

    strategy_cfg = _ensure_mapping(resolved.get("strategy"), name="strategy")
    strategy_cfg.setdefault("name", "coinflip")
    resolved["strategy"] = strategy_cfg

    execution_cfg = _ensure_mapping(resolved.get("execution"), name="execution")
    # Default execution profile is tier2. We do not implicitly switch to custom
    # when legacy override keys are present. Users must set profile=custom explicitly.
    execution_cfg.setdefault("profile", "tier2")
    execution_cfg.setdefault("spread_mode", "none")

    resolved["execution"] = execution_cfg

    audit_cfg = _ensure_mapping(resolved.get("audit"), name="audit")
    audit_cfg.setdefault("enabled", False)
    audit_cfg.setdefault("level", "basic")
    audit_cfg.setdefault("max_events_per_file", 5000)
    audit_cfg.setdefault("determinism_check", False)
    resolved["audit"] = audit_cfg
    intrabar_spec = parse_intrabar_spec(resolved)
    execution_cfg["intrabar_mode"] = intrabar_spec.mode

    spread_mode = execution_cfg.get("spread_mode")
    if spread_mode not in {"none", "fixed_bps", "bar_range_proxy", "fixed_pips"}:
        raise ConfigError(
            "Invalid execution.spread_mode: expected one of "
            "{'none', 'fixed_bps', 'bar_range_proxy', 'fixed_pips'} "
            f"got {spread_mode!r}"
        )

    if spread_mode == "fixed_bps":
        spread_bps_raw = execution_cfg.get("spread_bps")
        if spread_bps_raw is None:
            spread_bps = resolve_execution_profile(resolved).spread_bps
        else:
            try:
                spread_bps = float(spread_bps_raw)
            except (TypeError, ValueError) as exc:
                raise ConfigError("Invalid execution.spread_bps: expected float >= 0") from exc
        if spread_bps < 0:
            raise ConfigError("Invalid execution.spread_bps: expected float >= 0")
        if "spread_bps" in execution_cfg:
            execution_cfg["spread_bps"] = spread_bps

    if spread_mode == "fixed_pips":
        raw_spread_pips = execution_cfg.get("spread_pips")
        try:
            spread_pips = float(raw_spread_pips)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"execution.spread_pips must be > 0 (got: {raw_spread_pips!r})") from exc
        if spread_pips <= 0:
            raise ConfigError(f"execution.spread_pips must be > 0 (got: {raw_spread_pips!r})")
        execution_cfg["spread_pips"] = spread_pips

    if spread_mode in {"none", "bar_range_proxy"} and "spread_bps" in execution_cfg:
        try:
            execution_cfg["spread_bps"] = float(execution_cfg["spread_bps"])
        except (TypeError, ValueError) as exc:
            raise ConfigError("Invalid execution.spread_bps: expected float >= 0") from exc
        if execution_cfg["spread_bps"] < 0:
            raise ConfigError("Invalid execution.spread_bps: expected float >= 0")

    commission_cfg = _ensure_mapping(execution_cfg.get("commission"), name="execution.commission")
    commission_mode = commission_cfg.get("mode", "none")
    if commission_mode not in {"none", "per_trade", "per_share", "per_lot"}:
        raise ConfigError(
            "execution.commission.mode must be one of {'none', 'per_trade', 'per_share', 'per_lot'} "
            f"(got: {commission_mode!r})"
        )
    commission_cfg["mode"] = commission_mode
    for key in ("per_trade", "per_share", "per_lot"):
        if key in commission_cfg and commission_cfg[key] is not None:
            try:
                parsed = float(commission_cfg[key])
            except (TypeError, ValueError) as exc:
                raise ConfigError(f"execution.commission.{key} must be >= 0 (got: {commission_cfg[key]!r})") from exc
            if parsed < 0:
                raise ConfigError(f"execution.commission.{key} must be >= 0 (got: {commission_cfg[key]!r})")
            commission_cfg[key] = parsed
    execution_cfg["commission"] = commission_cfg

    instrument_cfg = resolved.get("instrument") if isinstance(resolved.get("instrument"), dict) else None
    instrument_type = instrument_cfg.get("type") if isinstance(instrument_cfg, dict) else None
    if instrument_type == "forex" and spread_mode == "none":
        raise ConfigError(
            "execution.spread_mode: FX V1 requires spread modeling: "
            "set execution.spread_mode=fixed_pips (or fixed_bps). Current execution.spread_mode=none."
        )
    if spread_mode == "fixed_pips" and instrument_type != "forex":
        raise ConfigError(
            "execution.spread_mode=fixed_pips is FX-only. "
            "Set instrument.type=forex or set execution.spread_mode=fixed_bps."
        )
    if commission_mode == "per_lot" and instrument_type != "forex":
        raise ConfigError(
            "execution.commission.mode=per_lot requires instrument.type=forex. "
            "Set instrument.type=forex or use execution.commission.mode=per_trade."
        )
    if commission_mode == "per_share" and instrument_type != "equity":
        raise ConfigError(
            "execution.commission.mode=per_share requires instrument.type=equity. "
            "Set instrument.type=equity or use execution.commission.mode=per_trade."
        )

    resolved["execution"] = execution_cfg

    benchmark_cfg = _ensure_mapping(resolved.get("benchmark"), name="benchmark")

    # New platform-managed benchmark contract for Strategy Robustness Lab.
    if "mode" in benchmark_cfg and benchmark_cfg.get("mode") in {"auto", "manual", "none"}:
        try:
            parsed_benchmark_cfg = parse_benchmark_config(benchmark_cfg)
        except BenchmarkConfigError as exc:
            raise ConfigError(str(exc)) from exc

        if parsed_benchmark_cfg.enabled:
            resolved["benchmark"] = {
                "enabled": True,
                "mode": parsed_benchmark_cfg.mode,
                "id": parsed_benchmark_cfg.id,
                "source": parsed_benchmark_cfg.source,
                "library_root": str(parsed_benchmark_cfg.library_root),
                "library_revision": parsed_benchmark_cfg.library_revision,
                "frequency": parsed_benchmark_cfg.frequency,
                "alignment_policy": parsed_benchmark_cfg.alignment_policy,
                "comparison_frequency": parsed_benchmark_cfg.comparison_frequency,
                "normalization_basis": parsed_benchmark_cfg.normalization_basis,
            }
        else:
            resolved["benchmark"] = {"enabled": False, "mode": "none"}
    else:
        # Legacy benchmark config contract remains supported for strategy benchmarking.
        enabled_raw = benchmark_cfg.get("enabled", False)
        if not isinstance(enabled_raw, bool):
            raise ConfigError(f"benchmark.enabled must be a bool (got: {enabled_raw!r})")
        benchmark_cfg["enabled"] = enabled_raw

        benchmark_type = benchmark_cfg.get("type")
        if enabled_raw and benchmark_type is None:
            benchmark_type = "buy_hold"
        elif benchmark_type is None:
            benchmark_type = "buy_hold"
        if benchmark_type not in {"buy_hold", "flat", "baseline_strategy"}:
            raise ConfigError(
                "benchmark.type must be one of {'buy_hold', 'flat', 'baseline_strategy'} "
                f"(got: {benchmark_type!r})"
            )
        benchmark_cfg["type"] = benchmark_type

        symbol = benchmark_cfg.get("symbol")
        if benchmark_type == "buy_hold" and enabled_raw:
            if not isinstance(symbol, str) or not symbol.strip():
                raise ConfigError(
                    "benchmark.symbol is required when benchmark.enabled=true and benchmark.type=buy_hold"
                )

        baseline_cfg = _ensure_mapping(benchmark_cfg.get("baseline_strategy"), name="benchmark.baseline_strategy")
        if benchmark_type == "baseline_strategy" and enabled_raw:
            baseline_name = baseline_cfg.get("name")
            if not isinstance(baseline_name, str) or not baseline_name.strip():
                raise ConfigError(
                    "benchmark.baseline_strategy.name is required when benchmark.type=baseline_strategy"
                )
            params = baseline_cfg.get("params", {})
            if not isinstance(params, dict):
                raise ConfigError("benchmark.baseline_strategy.params must be a mapping when provided")
            baseline_cfg["params"] = params
        benchmark_cfg["baseline_strategy"] = baseline_cfg

        resolved["benchmark"] = benchmark_cfg

    if "htf_timeframes" in resolved or "htf_strict" in resolved:
        htf_resampler_cfg = _ensure_mapping(resolved.get("htf_resampler"), name="htf_resampler")
        if "htf_timeframes" in resolved:
            htf_resampler_cfg.setdefault("timeframes", resolved.get("htf_timeframes"))
        if "htf_strict" in resolved:
            htf_resampler_cfg.setdefault("strict", resolved.get("htf_strict"))
        htf_resampler_cfg.setdefault("strict", True)
        resolved["htf_resampler"] = htf_resampler_cfg
        resolved.pop("htf_timeframes", None)
        resolved.pop("htf_strict", None)

    _resolve_risk_value(
        resolved=resolved,
        top_key="max_positions",
        nested_key="max_positions",
        default=1,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="max_leverage",
        nested_key="max_leverage",
        default=2.0,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="stop_resolution",
        nested_key="stop_resolution",
        default="safe",
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="allow_legacy_proxy",
        nested_key="allow_legacy_proxy",
        default=False,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="slippage_k",
        nested_key="slippage_k_proxy",
        default=0.0,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="margin_buffer_tier",
        nested_key="margin_buffer_tier",
        default=1,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="min_stop_distance_pct",
        nested_key="min_stop_distance_pct",
        default=0.001,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="max_notional_pct_equity",
        nested_key="max_notional_pct_equity",
        default=0.5,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="max_gross_notional_pct_equity",
        nested_key="max_gross_notional_pct_equity",
        default=0.5,
    )
    _resolve_risk_value(
        resolved=resolved,
        top_key="maintenance_free_margin_pct",
        nested_key="maintenance_free_margin_pct",
        default=0.01,
    )
    _resolve_sizing_block(resolved)
    _resolve_r_per_trade_alias(resolved)

    risk_cfg = resolved.get("risk", {})
    risk_cfg.setdefault("mode", "equity_pct")
    risk_cfg.setdefault("cap_policy", "allow_clip_with_truth")
    risk_cfg.setdefault("min_risk_utilization_pct", 0.0)
    risk_cfg.setdefault("report_under_risked_trades", True)
    risk_cfg.setdefault("may_liquidate", True)
    fx_cfg = _ensure_mapping(risk_cfg.get("fx"), name="risk.fx")
    fx_cfg.setdefault("lot_step", None)
    fx_cfg.setdefault("pip_value_override", None)
    risk_cfg["fx"] = fx_cfg

    margin_cfg = _ensure_mapping(risk_cfg.get("margin"), name="risk.margin")
    margin_cfg.setdefault("leverage", None)
    risk_cfg["margin"] = margin_cfg
    stop_resolution = risk_cfg.get("stop_resolution")
    if stop_resolution not in {"safe", "strict", "allow_legacy_proxy"}:
        raise ConfigError(
            "Invalid risk.stop_resolution: expected one of safe, strict "
            "(or legacy allow_legacy_proxy)"
        )

    allow_legacy_proxy = risk_cfg.get("allow_legacy_proxy")
    if not isinstance(allow_legacy_proxy, bool):
        raise ConfigError("Invalid risk.allow_legacy_proxy: expected boolean")

    if stop_resolution == "allow_legacy_proxy":
        stop_resolution = "safe"
        allow_legacy_proxy = True
        warnings = resolved.setdefault("warnings", [])
        if not isinstance(warnings, list):
            raise ConfigError("Invalid warnings: expected list")
        if _LEGACY_STOP_RESOLUTION_WARNING not in warnings:
            warnings.append(_LEGACY_STOP_RESOLUTION_WARNING)

    if stop_resolution == "strict" and allow_legacy_proxy:
        raise ConfigError(
            "Invalid config: risk.allow_legacy_proxy=true is not allowed when "
            "risk.stop_resolution=strict"
        )

    risk_cfg["stop_resolution"] = stop_resolution
    risk_cfg["allow_legacy_proxy"] = allow_legacy_proxy

    if not isinstance(risk_cfg.get("may_liquidate"), bool):
        raise ConfigError("Invalid risk.may_liquidate: expected boolean")

    if risk_cfg.get("cap_policy") not in {"allow_clip_with_truth", "reject_if_clipped"}:
        raise ConfigError(
            "Invalid risk.cap_policy: expected allow_clip_with_truth or reject_if_clipped"
        )

    try:
        min_risk_utilization_pct = float(risk_cfg.get("min_risk_utilization_pct"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.min_risk_utilization_pct: expected a float in [0.0, 1.0]; "
            f"got {risk_cfg.get('min_risk_utilization_pct')!r}."
        ) from exc
    if not (0.0 <= min_risk_utilization_pct <= 1.0):
        raise ConfigError(
            "Invalid risk.min_risk_utilization_pct: expected a float in [0.0, 1.0] "
            f"got {min_risk_utilization_pct!r}."
        )
    risk_cfg["min_risk_utilization_pct"] = min_risk_utilization_pct

    if not isinstance(risk_cfg.get("report_under_risked_trades"), bool):
        raise ConfigError("Invalid risk.report_under_risked_trades: expected boolean")

    if risk_cfg.get("mode") == "fixed_notional_pct_equity":
        try:
            notional_pct_equity = float(risk_cfg.get("notional_pct_equity"))
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "Invalid risk.notional_pct_equity: expected a float in (0.0, 5.0]; "
                f"got {risk_cfg.get('notional_pct_equity')!r}."
            ) from exc
        if not (0.0 < notional_pct_equity <= 5.0):
            raise ConfigError(
                "Invalid risk.notional_pct_equity: expected a float in (0.0, 5.0] "
                f"got {notional_pct_equity!r}."
            )
        risk_cfg["notional_pct_equity"] = notional_pct_equity

    try:
        margin_buffer_tier = int(risk_cfg.get("margin_buffer_tier"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.margin_buffer_tier: expected one of {1, 2, 3}; "
            f"got {risk_cfg.get('margin_buffer_tier')!r}."
        ) from exc
    if margin_buffer_tier not in {1, 2, 3}:
        raise ConfigError(
            "Invalid risk.margin_buffer_tier: expected one of {1, 2, 3} "
            f"got {margin_buffer_tier!r}. "
            "Set risk.margin_buffer_tier explicitly to 1 (no proxy buffer), 2, or 3."
        )
    risk_cfg["margin_buffer_tier"] = margin_buffer_tier

    try:
        slippage_k_proxy = float(risk_cfg.get("slippage_k_proxy"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.slippage_k_proxy: expected a value in [0.0, 0.05]; "
            f"got {risk_cfg.get('slippage_k_proxy')!r}."
        ) from exc
    if not (0.0 <= slippage_k_proxy <= 0.05):
        raise ConfigError(
            "Invalid risk.slippage_k_proxy: expected a value in [0.0, 0.05] "
            f"got {slippage_k_proxy!r}. "
            "Use 0.0 to disable the proxy buffer or a small fraction like 0.001."
        )
    risk_cfg["slippage_k_proxy"] = slippage_k_proxy

    try:
        min_stop_distance_pct = float(risk_cfg.get("min_stop_distance_pct"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.min_stop_distance_pct: expected a float in [0.0, 0.05]; "
            f"got {risk_cfg.get('min_stop_distance_pct')!r}."
        ) from exc
    if not (0.0 <= min_stop_distance_pct <= 0.05):
        raise ConfigError(
            "Invalid risk.min_stop_distance_pct: expected a float in [0.0, 0.05] "
            f"got {min_stop_distance_pct!r}. "
            "Use 0.0 to disable this guardrail or a small fraction like 0.001 (0.1%)."
        )
    risk_cfg["min_stop_distance_pct"] = min_stop_distance_pct

    try:
        max_notional_pct_equity = float(risk_cfg.get("max_notional_pct_equity"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.max_notional_pct_equity: expected a float in (0.0, 5.0]; "
            f"got {risk_cfg.get('max_notional_pct_equity')!r}."
        ) from exc
    if not (0.0 < max_notional_pct_equity <= 5.0):
        raise ConfigError(
            "Invalid risk.max_notional_pct_equity: expected a float in (0.0, 5.0] "
            f"got {max_notional_pct_equity!r}. "
            "Set it to 1.0 for a 100% of equity cap or increase up to 5.0 when needed."
        )
    risk_cfg["max_notional_pct_equity"] = max_notional_pct_equity

    raw_max_gross_notional_pct_equity = risk_cfg.get("max_gross_notional_pct_equity")
    if raw_max_gross_notional_pct_equity is None:
        max_gross_notional_pct_equity = None
    else:
        try:
            max_gross_notional_pct_equity = float(raw_max_gross_notional_pct_equity)
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                "Invalid risk.max_gross_notional_pct_equity: expected null or a float in (0.0, 5.0]; "
                f"got {raw_max_gross_notional_pct_equity!r}."
            ) from exc
        if not (0.0 < max_gross_notional_pct_equity <= 5.0):
            raise ConfigError(
                "Invalid risk.max_gross_notional_pct_equity: expected null or a float in (0.0, 5.0] "
                f"got {max_gross_notional_pct_equity!r}. "
                "Set it to null to disable or to max_positions * max_notional_pct_equity for a tight gross cap."
            )
    risk_cfg["max_gross_notional_pct_equity"] = max_gross_notional_pct_equity

    try:
        maintenance_free_margin_pct = float(risk_cfg.get("maintenance_free_margin_pct"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            "Invalid risk.maintenance_free_margin_pct: expected a float in [0.0, 0.20]; "
            f"got {risk_cfg.get('maintenance_free_margin_pct')!r}."
        ) from exc
    if not (0.0 <= maintenance_free_margin_pct <= 0.20):
        raise ConfigError(
            "Invalid risk.maintenance_free_margin_pct: expected a float in [0.0, 0.20] "
            f"got {maintenance_free_margin_pct!r}. "
            "Set it to 0.01 for a 1% maintenance free-margin floor."
        )
    risk_cfg["maintenance_free_margin_pct"] = maintenance_free_margin_pct

    fx_cfg = _ensure_mapping(risk_cfg.get("fx"), name="risk.fx")
    lot_step = fx_cfg.get("lot_step")
    if lot_step is not None:
        try:
            lot_step_value = float(lot_step)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"risk.fx.lot_step must be > 0 (got: {lot_step!r})") from exc
        if lot_step_value <= 0:
            raise ConfigError(f"risk.fx.lot_step must be > 0 (got: {lot_step!r})")
        fx_cfg["lot_step"] = lot_step_value

    pip_value_override = fx_cfg.get("pip_value_override")
    if pip_value_override is not None:
        try:
            pip_value = float(pip_value_override)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"risk.fx.pip_value_override must be > 0 (got: {pip_value_override!r})") from exc
        if pip_value <= 0:
            raise ConfigError(f"risk.fx.pip_value_override must be > 0 (got: {pip_value_override!r})")
        fx_cfg["pip_value_override"] = pip_value
    risk_cfg["fx"] = fx_cfg

    margin_cfg = _ensure_mapping(risk_cfg.get("margin"), name="risk.margin")
    margin_leverage = margin_cfg.get("leverage")
    if margin_leverage is not None:
        try:
            margin_leverage_value = float(margin_leverage)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"risk.margin.leverage must be > 0 (got: {margin_leverage!r})") from exc
        if margin_leverage_value <= 0:
            raise ConfigError(f"risk.margin.leverage must be > 0 (got: {margin_leverage!r})")
        margin_cfg["leverage"] = margin_leverage_value
    risk_cfg["margin"] = margin_cfg

    instrument_cfg = resolved.get("instrument") if isinstance(resolved.get("instrument"), dict) else None
    instrument_type = instrument_cfg.get("type") if isinstance(instrument_cfg, dict) else None
    if instrument_type == "forex":
        contract_size = instrument_cfg.get("contract_size") if isinstance(instrument_cfg, dict) else None
        if contract_size is None:
            raise ConfigError(
                "instrument.contract_size is required when instrument.type=forex. "
                "Set instrument.contract_size (e.g., 100000)."
            )
        if fx_cfg.get("lot_step") is None:
            raise ConfigError(
                "risk.fx.lot_step is required when instrument.type=forex. "
                "Set risk.fx.lot_step (e.g., 0.01 for micro lots)."
            )

    resolved["risk"] = risk_cfg

    return resolved
