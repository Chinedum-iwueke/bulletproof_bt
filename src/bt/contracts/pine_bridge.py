"""Deterministic Pine v6 visualization, restricted import, and parity contracts."""
from __future__ import annotations

from hashlib import sha256
import json
import re
from typing import Any

from bt.contracts.qualification import classify_targets


PINE_COMPILER_VERSION = "pine_bridge_v1"
PORTABILITY_REGISTRY = {
    "identity": {"pine_supported": True, "pine": "{source}", "simulation": True},
    "return": {"pine_supported": True, "pine": "ta.change({source}) / {source}[1]", "simulation": True},
    "sma": {"pine_supported": True, "pine": "ta.sma({source}, {window})", "simulation": True},
    "ema": {"pine_supported": True, "pine": "ta.ema({source}, {window})", "simulation": True},
    "atr": {"pine_supported": True, "pine": "ta.atr({window})", "simulation": True},
    "true_range": {"pine_supported": True, "pine": "ta.tr(true)", "simulation": True},
    "true_range_over": {"pine_supported": True, "pine": "ta.tr(true) / {input}", "simulation": True},
    "half_range_over_close": {"pine_supported": True, "pine": "0.5 * (high - low) / close", "simulation": True},
}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: Any) -> str:
    return sha256((_canonical(value) if not isinstance(value, str) else value).encode()).hexdigest()


def pine_compatibility(portable_ir: dict[str, Any]) -> dict[str, Any]:
    targets = classify_targets(portable_ir)
    primitives = []
    unsupported = []
    for item in portable_ir.get("features", []):
        transform = str(item.get("transform"))
        meta = PORTABILITY_REGISTRY.get(transform)
        primitives.append({"feature_id": item.get("id"), "primitive": transform, **(meta or {"pine_supported": False})})
        if not meta:
            unsupported.append(f"primitive:{transform}")
    unsupported.extend(f"dataset:{item}" for item in portable_ir.get("data_requirements", []) if item not in {"ohlcv"})
    semantics = portable_ir.get("execution_semantics", {})
    if semantics.get("signal_bar_policy", "closed_bar_only") != "closed_bar_only":
        unsupported.append("unconfirmed_bar_semantics")
    if semantics.get("interpolation") != "forbidden":
        unsupported.append("interpolation_semantics")
    status = "unsupported" if unsupported or targets["visualization"] == "unsupported" else "visualization_compatible"
    simulation = "simulation_compatible" if status != "unsupported" and targets["simulation"] == "compatible" else "unsupported"
    report = {"schema_version": "pine_compatibility_report_v1", "status": status, "simulation_status": simulation, "portable_primitives": primitives, "unsupported": sorted(set(unsupported)), "approximations": ["TradingView chart data and alerts are not engine execution evidence."], "session_mapping": semantics.get("session", "24x7_crypto"), "timezone_mapping": "UTC", "timeframe_mapping": semantics.get("signal_timeframe"), "risk_omissions": ["account and portfolio risk remain engine-owned"], "compiler_version": PINE_COMPILER_VERSION}
    return {**report, "report_hash": _hash(report)}


def compile_pine_v6(portable_ir: dict[str, Any], *, export_id: str, program_id: str, approved: bool, generated_at: str) -> dict[str, Any]:
    if not approved:
        raise ValueError("approved_strategy_spec_required")
    compatibility = pine_compatibility(portable_ir)
    if compatibility["status"] == "unsupported":
        raise ValueError("pine_visualization_unsupported:" + ",".join(compatibility["unsupported"]))
    parameters = portable_ir.get("parameters", {})
    display_name = re.sub(r'["\\\r\n]', "_", str(portable_ir["strategy_spec_id"]))[:120]
    lines = ["//@version=6", f"// Invariance Research visualization | compiler={PINE_COMPILER_VERSION}", f"// program={program_id} | strategy_spec_hash={portable_ir['strategy_spec_hash']}", "// TradingView output is not an engine validation result.", f'indicator("{display_name} visualization", overlay=true, max_labels_count=200)']
    for key, value in sorted(parameters.items()):
        safe_key = re.sub(r"[^A-Za-z0-9_]", "_", str(key))
        if isinstance(value, bool):
            lines.append(f"p_{safe_key} = input.bool({str(value).lower()}, \"{key}\")")
        elif isinstance(value, (int, float)):
            candidates = [item for item in portable_ir.get("parameter_grid", {}).get(key, []) if isinstance(item, (int, float))]
            bounds = [float(value), *map(float, candidates)]
            lines.append(f"p_{safe_key} = input.float({value}, \"{key}\", minval={min(bounds)}, maxval={max(bounds)})")
    for feature in portable_ir.get("features", []):
        feature_id = re.sub(r"[^A-Za-z0-9_]", "_", str(feature["id"]))
        transform = str(feature["transform"])
        template = PORTABILITY_REGISTRY[transform]["pine"]
        source = str(feature.get("source_field", "close"))
        if source not in {"open", "high", "low", "close", "volume"}:
            raise ValueError(f"pine_source_field_unsupported:{source}")
        inputs = feature.get("inputs", [])
        expression = template.format(source=source, window=int(feature.get("window", 1)), input=re.sub(r"[^A-Za-z0-9_]", "_", str(inputs[0])) if inputs else "close")
        lag = int(feature.get("lag", 0))
        lines.append(f"{feature_id} = ({expression})" + (f"[{lag}]" if lag else ""))
    gate_names = []
    for index, gate in enumerate(portable_ir.get("gates", [])):
        if str(gate.get("op")) not in {">", ">=", "<", "<=", "=="}:
            raise ValueError(f"pine_gate_operator_unsupported:{gate.get('op')}")
        left = re.sub(r"[^A-Za-z0-9_]", "_", str(gate.get("left", gate.get("field"))))
        right = gate.get("right")
        if right is None:
            right = f'p_{re.sub(r"[^A-Za-z0-9_]", "_", str(gate.get("right_param", gate.get("param"))))}'
        elif not isinstance(right, (int, float)):
            right = re.sub(r"[^A-Za-z0-9_]", "_", str(right))
        gate_name = f"gate_{index}"
        lines.append(f"{gate_name} = {left} {gate['op']} {right}")
        gate_names.append(gate_name)
    lines += [
        f"confirmed_signal = barstate.isconfirmed and {' and '.join(gate_names) if gate_names else 'false'}",
        "long_signal = confirmed_signal and close >= open",
        "short_signal = confirmed_signal and close < open",
        "atr_visual = ta.atr(14)",
        "stop_multiple = 2.0",
        "long_stop = long_signal ? close - atr_visual * stop_multiple : na",
        "short_stop = short_signal ? close + atr_visual * stop_multiple : na",
        'plot(long_stop, title="Long invalidation", color=color.red, style=plot.style_linebr)',
        'plot(short_stop, title="Short invalidation", color=color.red, style=plot.style_linebr)',
        'plotshape(long_signal, title="Long", style=shape.triangleup, location=location.belowbar, color=color.green)',
        'plotshape(short_signal, title="Short", style=shape.triangledown, location=location.abovebar, color=color.red)',
        "var status_table = table.new(position.top_right, 1, 2)",
        "if barstate.islast",
        '    table.cell(status_table, 0, 0, "Invariance visualization")',
        f'    table.cell(status_table, 0, 1, "Spec {portable_ir["strategy_spec_hash"][:12]} | provisional")',
        f'''alertcondition(long_signal, title="Confirmed long", message="{{\\\"idempotency_key\\\":\\\"{export_id}:{{{{ticker}}}}:{{{{interval}}}}:{{{{time}}}}:long\\\",\\\"symbol\\\":\\\"{{{{ticker}}}}\\\",\\\"timeframe\\\":\\\"{{{{interval}}}}\\\",\\\"confirmed_bar_timestamp\\\":\\\"{{{{time}}}}\\\",\\\"side\\\":\\\"long\\\",\\\"event_type\\\":\\\"entry\\\",\\\"strategy_spec_hash\\\":\\\"{portable_ir["strategy_spec_hash"]}\\\",\\\"confirmed\\\":true}}")''',
        f'''alertcondition(short_signal, title="Confirmed short", message="{{\\\"idempotency_key\\\":\\\"{export_id}:{{{{ticker}}}}:{{{{interval}}}}:{{{{time}}}}:short\\\",\\\"symbol\\\":\\\"{{{{ticker}}}}\\\",\\\"timeframe\\\":\\\"{{{{interval}}}}\\\",\\\"confirmed_bar_timestamp\\\":\\\"{{{{time}}}}\\\",\\\"side\\\":\\\"short\\\",\\\"event_type\\\":\\\"entry\\\",\\\"strategy_spec_hash\\\":\\\"{portable_ir["strategy_spec_hash"]}\\\",\\\"confirmed\\\":true}}")''',
    ]
    source = "\n".join(lines) + "\n"
    if len(source.encode()) > 100_000 or "lookahead_on" in source or re.search(r"\[-\d+\]", source):
        raise ValueError("pine_static_policy_failed")
    simulation_source = None
    if compatibility["simulation_status"] == "simulation_compatible":
        stop_param = re.sub(r"[^A-Za-z0-9_]", "_", str(portable_ir["exit"]["stop_param"]))
        hold_param = re.sub(r"[^A-Za-z0-9_]", "_", str(portable_ir["exit"]["max_hold_param"]))
        simulation_logic = f'''var int entry_bar = na
if long_signal
    strategy.entry("Long", strategy.long)
    entry_bar := bar_index
if short_signal
    strategy.entry("Short", strategy.short)
    entry_bar := bar_index
if strategy.position_size > 0
    strategy.exit("Long stop", "Long", stop=strategy.position_avg_price - atr_visual * p_{stop_param})
if strategy.position_size < 0
    strategy.exit("Short stop", "Short", stop=strategy.position_avg_price + atr_visual * p_{stop_param})
if strategy.position_size != 0 and not na(entry_bar) and bar_index - entry_bar >= p_{hold_param}
    strategy.close_all(comment="Time exit")
if strategy.position_size == 0
    entry_bar := na'''
        simulation_source = source.replace(
            f'indicator("{display_name} visualization", overlay=true, max_labels_count=200)',
            f'strategy("{display_name} simulation", overlay=true, pyramiding=0, commission_type=strategy.commission.percent, commission_value=0.1, slippage=1)',
        ).replace(
            'plotshape(long_signal, title="Long", style=shape.triangleup, location=location.belowbar, color=color.green)',
            simulation_logic + '\nplotshape(long_signal, title="Long", style=shape.triangleup, location=location.belowbar, color=color.green)',
        )
    files = {"strategy_visualization.pine": _hash(source)}
    if simulation_source:
        files["strategy_simulation.pine"] = _hash(simulation_source)
    manifest = {"schema_version": "pine_export_manifest_v1", "export_id": export_id, "program_id": program_id, "strategy_spec_id": portable_ir["strategy_spec_id"], "strategy_spec_hash": portable_ir["strategy_spec_hash"], "pine_version": "v6", "compiler_version": PINE_COMPILER_VERSION, "generated_at": generated_at, "approval_state": "approved", "files": files, "source_sharing": "account_private"}
    parity = {"schema_version": "pine_parity_report_v1", "comparison_source": "reference_evaluator", "verdict": "provisional", "reason": "No TradingView export has been compared.", "matched": 0, "missing": 0, "extra": 0, "direction_mismatches": 0, "tolerance_policy": {"timestamp_seconds": 0}}
    readme = """# TradingView setup

1. Open the exact symbol and timeframe recorded in the compatibility report.
2. Paste strategy_visualization.pine into Pine Editor, save it, and add it to the chart.
3. Match every bounded input to the approved Strategy Spec snapshot.
4. Create confirmed-bar alerts only after issuing a webhook credential in Research Desk.
5. Recreate the script and every alert whenever the spec, script, symbol, timeframe, session, or input changes.
6. Export matching signals and run parity before treating the visualization as semantically aligned.

strategy_simulation.pine is present only for the portable simulation subset. TradingView chart data, fills, costs, and broker emulator behavior are not Bulletproof engine evidence. Alerts are observation-only and cannot create orders or authorize deployment.
"""
    bundle = {"source": source, "simulation_source": simulation_source, "manifest": manifest, "compatibility": compatibility, "parity": parity, "strategy_spec_snapshot": portable_ir, "readme": readme}
    return {**bundle, "bundle_hash": _hash(bundle)}


def compare_signals(*, engine: list[dict[str, Any]], tradingview: list[dict[str, Any]], context: dict[str, Any]) -> dict[str, Any]:
    required = ("symbol", "timeframe", "window_start", "window_end", "timezone", "session", "parameter_hash")
    missing_context = [key for key in required if not context.get(key)]
    if missing_context:
        raise ValueError("parity_context_missing:" + ",".join(missing_context))
    left = {str(item["timestamp"]): str(item["side"]).lower() for item in engine}
    right = {str(item["timestamp"]): str(item["side"]).lower() for item in tradingview}
    common = sorted(set(left) & set(right))
    direction_mismatches = [{"timestamp": timestamp, "engine_side": left[timestamp], "tradingview_side": right[timestamp]} for timestamp in common if left[timestamp] != right[timestamp]]
    missing = [{"timestamp": timestamp, "side": left[timestamp]} for timestamp in sorted(set(left) - set(right))]
    extra = [{"timestamp": timestamp, "side": right[timestamp]} for timestamp in sorted(set(right) - set(left))]
    divergences = sorted([*missing, *extra, *direction_mismatches], key=lambda item: item["timestamp"])
    verdict = "verified" if not divergences else "divergent"
    report = {"schema_version": "pine_parity_report_v1", "comparison_source": "tradingview_export", **context, "engine_signal_count": len(engine), "tradingview_signal_count": len(tradingview), "matched": len(common) - len(direction_mismatches), "missing": missing, "extra": extra, "direction_mismatches": direction_mismatches, "first_divergence": divergences[0] if divergences else None, "tolerance_policy": {"timestamp_seconds": 0}, "verdict": verdict}
    return {**report, "report_hash": _hash(report)}


def evaluate_portable_signals(portable_ir: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reference closed-bar evaluator for Pine parity fixtures, never for execution."""
    values: dict[str, list[float | None]] = {}
    for feature in portable_ir.get("features", []):
        transform = str(feature.get("transform"))
        source = [float(row.get(str(feature.get("source_field", "close")), 0.0)) for row in rows]
        window = int(feature.get("window", 1))
        base: list[float | None] = []
        computed: list[float | None] = []
        for index, value in enumerate(source):
            if transform == "identity":
                result: float | None = value
            elif transform == "return":
                previous = source[index - 1] if index else 0.0
                result = (value - previous) / previous if previous else None
            elif transform == "sma":
                result = sum(source[index - window + 1:index + 1]) / window if index + 1 >= window else None
            elif transform == "ema":
                previous_ema = base[-1] if base else None
                result = value if previous_ema is None else value * (2 / (window + 1)) + previous_ema * (1 - 2 / (window + 1))
            else:
                raise ValueError(f"portable_reference_unsupported:{transform}")
            lag = int(feature.get("lag", 0))
            base.append(result)
            computed.append(base[index - lag] if lag and index >= lag else None if lag else result)
        values[str(feature["id"])] = computed
    parameters = portable_ir.get("parameters", {})
    signals = []
    operators = {">": lambda a,b:a>b, ">=":lambda a,b:a>=b, "<":lambda a,b:a<b, "<=":lambda a,b:a<=b, "==":lambda a,b:a==b}
    for index, row in enumerate(rows):
        passed = True
        for gate in portable_ir.get("gates", []):
            left = values.get(str(gate.get("left", gate.get("field"))), [None] * len(rows))[index]
            right = gate.get("right", parameters.get(str(gate.get("right_param", gate.get("param")))))
            if isinstance(right, list):
                right = right[0] if right else None
            if left is None or right is None or str(gate.get("op")) not in operators or not operators[str(gate.get("op"))](float(left), float(right)):
                passed = False
                break
        if passed:
            signals.append({"timestamp": row.get("timestamp"), "side": "long" if float(row.get("close", 0)) >= float(row.get("open", 0)) else "short"})
    return signals


def parse_restricted_pine(source: str) -> dict[str, Any]:
    if len(source.encode()) > 200_000:
        raise ValueError("pine_source_too_large")
    version = re.search(r"//@version=(\d+)", source)
    rejected = {token for token in ("strategy(", "strategy.entry", "strategy.order", "strategy.exit", "request.security", "request.seed", "import ", "library(", "array.", "map.", "matrix.") if token in source}
    allowed_calls = {"indicator", "input.float", "input.int", "input.bool", "ta.sma", "ta.ema", "ta.atr", "plot", "plotshape", "alertcondition"}
    rejected.update(f"unsupported_call:{name}" for name in re.findall(r"\b([A-Za-z_][A-Za-z0-9_.]*)\s*\(", source) if name not in allowed_calls)
    rejected = sorted(rejected)
    supported_nodes = sorted({token for token in ("indicator(", "input.float", "input.int", "input.bool", "ta.sma", "ta.ema", "ta.atr", "plotshape", "alertcondition") if token in source})
    params = [{"name": match.group(2), "default": float(match.group(1))} for match in re.finditer(r"input\.float\(([-+0-9.eE]+),\s*[\"']([^\"']+)", source)]
    report = {"schema_version": "pine_import_report_v1", "source_checksum": _hash(source), "detected_pine_version": f"v{version.group(1)}" if version else "unknown", "supported_ast_nodes": supported_nodes, "rejected_constructs": rejected, "extracted_parameters": params, "extracted_signals": [item for item in ("long_signal", "short_signal") if item in source], "ambiguities": ["Pine execution and chart-data semantics require user confirmation."], "draft_spec_status": "blocked" if rejected or not version or version.group(1) != "6" else "draft"}
    return {**report, "report_hash": _hash(report)}
