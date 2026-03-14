from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from bt.logging.formatting import write_text_deterministic


def _read_json_file(run_dir: Path, filename: str, *, required: bool) -> dict[str, Any] | None:
    path = run_dir / filename
    if not path.exists():
        if required:
            raise ValueError(f"Missing required artifact for run_dir={run_dir}: {filename}")
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed artifact for run_dir={run_dir}: {filename}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Malformed artifact for run_dir={run_dir}: {filename} must contain a JSON object")
    return payload


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return "None"
    return f"{numeric:.6f}"


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        numeric = _to_float(value)
        if numeric is None:
            return None
        return int(numeric)


def _read_trades_stats(run_dir: Path) -> tuple[int, int | None, float | None]:
    trades_path = run_dir / "trades.csv"
    if not trades_path.exists():
        raise ValueError(f"Missing required artifact for run_dir={run_dir}: trades.csv")

    try:
        with trades_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError(f"Malformed artifact for run_dir={run_dir}: trades.csv missing header row")

            trade_count = 0
            longest_loss_streak = 0
            current_loss_streak = 0
            worst_trade_r: float | None = None
            r_column: str | None = None
            if "r_multiple_net" in reader.fieldnames:
                r_column = "r_multiple_net"
            elif "r_multiple_gross" in reader.fieldnames:
                r_column = "r_multiple_gross"

            for row in reader:
                trade_count += 1
                pnl_value = _to_float(row.get("pnl_net"))
                if pnl_value is None:
                    pnl_value = _to_float(row.get("pnl"))
                if pnl_value is not None and pnl_value < 0:
                    current_loss_streak += 1
                    longest_loss_streak = max(longest_loss_streak, current_loss_streak)
                else:
                    current_loss_streak = 0

                if r_column is not None:
                    r_value = _to_float(row.get(r_column))
                    if r_value is not None:
                        if worst_trade_r is None or r_value < worst_trade_r:
                            worst_trade_r = r_value
    except csv.Error as exc:
        raise ValueError(f"Malformed artifact for run_dir={run_dir}: trades.csv: {exc}") from exc

    return trade_count, longest_loss_streak, worst_trade_r


def _is_benchmark_enabled(run_dir: Path, run_status: dict[str, Any] | None) -> bool:
    run_manifest = _read_json_file(run_dir, "run_manifest.json", required=False)
    if run_manifest is not None and isinstance(run_manifest.get("benchmark_enabled"), bool):
        return bool(run_manifest["benchmark_enabled"])

    if run_status is not None:
        raw_flag = run_status.get("benchmark_enabled")
        if isinstance(raw_flag, bool):
            return raw_flag

    return any(
        (run_dir / artifact).exists()
        for artifact in ("benchmark_metrics.json", "comparison_summary.json", "benchmark_equity.csv")
    )


def _extract_comparison_totals(
    perf: dict[str, Any], comparison: dict[str, Any] | None, benchmark_metrics: dict[str, Any] | None
) -> tuple[float | None, float | None, float | None]:
    strategy_total = _to_float(perf.get("total_return"))
    if strategy_total is None:
        strategy_total = _to_float(perf.get("total_return_pct"))

    benchmark_total: float | None = None
    excess_return: float | None = None

    if comparison is not None:
        strategy_block = comparison.get("strategy")
        benchmark_block = comparison.get("benchmark")
        delta_block = comparison.get("delta")

        if isinstance(strategy_block, dict):
            strategy_total = _to_float(strategy_block.get("total_return")) or strategy_total
        if isinstance(benchmark_block, dict):
            benchmark_total = _to_float(benchmark_block.get("total_return"))
        if isinstance(delta_block, dict):
            excess_return = _to_float(delta_block.get("total_return"))

    if benchmark_total is None and benchmark_metrics is not None:
        benchmark_total = _to_float(benchmark_metrics.get("total_return"))

    if excess_return is None and strategy_total is not None and benchmark_total is not None:
        excess_return = strategy_total - benchmark_total

    return strategy_total, benchmark_total, excess_return


def _comparison_value(perf: dict[str, Any], key_options: tuple[str, ...]) -> float | None:
    for key in key_options:
        value = _to_float(perf.get(key))
        if value is not None:
            return value
    return None


def derive_conclusion(perf: dict[str, Any], comparison: dict[str, Any] | None) -> str:
    sharpe = _comparison_value(perf, ("sharpe", "sharpe_annualized"))
    max_drawdown_pct = _comparison_value(perf, ("max_drawdown_pct",))
    if max_drawdown_pct is None:
        max_drawdown = _comparison_value(perf, ("max_drawdown",))
        if max_drawdown is not None:
            max_drawdown_pct = max_drawdown * 100.0 if max_drawdown <= 1.0 else max_drawdown

    if comparison is not None:
        excess_return: float | None = None
        delta_block = comparison.get("delta")
        if isinstance(delta_block, dict):
            excess_return = _to_float(delta_block.get("total_return"))

        if (
            excess_return is not None
            and excess_return > 0
            and sharpe is not None
            and sharpe >= 1.0
            and max_drawdown_pct is not None
            and max_drawdown_pct <= 25.0
        ):
            return "Conclusion: Outperformed benchmark with acceptable risk-adjusted returns."
        if excess_return is not None and excess_return <= 0:
            return "Conclusion: Underperformed benchmark; edge not validated under current costs/execution."
        return "Conclusion: Mixed results vs benchmark; investigate regime filters and robustness."

    net_pnl = _comparison_value(perf, ("net_pnl",))
    profit_factor = _comparison_value(perf, ("profit_factor", "profit_factor_r"))

    if net_pnl is not None and net_pnl > 0 and profit_factor is not None and profit_factor >= 1.2:
        return "Conclusion: Profitable under modeled costs; validate robustness and regime dependence."
    if net_pnl is not None and net_pnl <= 0:
        return "Conclusion: Not profitable under modeled costs; refine signal quality or risk/execution assumptions."
    return "Conclusion: Inconclusive; inspect trade distribution and stability across parameters."


def _parse_stop_contract(run_status: dict[str, Any] | None) -> dict[str, Any] | None:
    if run_status is None:
        return None
    payload = run_status.get("stop_contract")
    if not isinstance(payload, dict):
        return None
    counts = payload.get("counts")
    if not isinstance(counts, dict):
        return None
    return payload


def _stop_contract_hint(counts: dict[str, int]) -> str | None:
    hint_priority: list[tuple[str, str]] = [
        (
            "rejected_missing_stop",
            "Hint: In strict mode, entry signals must provide stop_price or metadata.stop_spec.",
        ),
        (
            "rejected_unresolvable_stop",
            "Hint: Ensure provided stop_spec is resolvable with current bar/context inputs.",
        ),
        (
            "rejected_invalid_stop_payload",
            "Hint: Validate stop payload fields/types for stop_price or metadata.stop_spec.",
        ),
    ]
    for key, hint in hint_priority:
        if counts.get(key, 0) > 0:
            return hint
    return None


def write_summary_txt(run_dir: Path) -> Path:
    """
    Reads run artifacts and writes summary.txt to run_dir.
    Returns the path to summary.txt.
    """
    if not run_dir.exists() or not run_dir.is_dir():
        raise ValueError(f"run_dir must exist and be a directory: {run_dir}")

    perf = _read_json_file(run_dir, "performance.json", required=True)
    run_status = _read_json_file(run_dir, "run_status.json", required=False)

    trades_count, loss_streak_from_trades, worst_trade_r = _read_trades_stats(run_dir)

    longest_loss_streak = _safe_int(perf.get("longest_loss_streak"))
    if longest_loss_streak is None:
        longest_loss_streak = _safe_int(perf.get("max_consecutive_losses"))
    if longest_loss_streak is None:
        longest_loss_streak = loss_streak_from_trades

    max_drawdown_duration = _safe_int(perf.get("max_drawdown_duration"))
    if max_drawdown_duration is None:
        max_drawdown_duration = _safe_int(perf.get("max_drawdown_duration_bars"))

    comparison = _read_json_file(run_dir, "comparison_summary.json", required=False)
    benchmark_metrics = _read_json_file(run_dir, "benchmark_metrics.json", required=False)
    benchmark_enabled = _is_benchmark_enabled(run_dir, run_status)

    if benchmark_enabled and comparison is None and benchmark_metrics is None:
        raise ValueError(
            "Missing benchmark artifact(s) for "
            f"run_dir={run_dir}: expected one of comparison_summary.json or benchmark_metrics.json"
        )

    strategy_total, benchmark_total, excess_return = _extract_comparison_totals(
        perf,
        comparison,
        benchmark_metrics,
    )

    top_total_return = _to_float(perf.get("total_return"))
    if top_total_return is None:
        top_total_return = _to_float(perf.get("total_return_pct"))

    top_max_drawdown = _to_float(perf.get("max_drawdown"))
    if top_max_drawdown is None:
        top_max_drawdown = _to_float(perf.get("max_drawdown_pct"))

    sharpe = _comparison_value(perf, ("sharpe", "sharpe_annualized"))
    sortino = _comparison_value(perf, ("sortino", "sortino_annualized"))
    mar = _comparison_value(perf, ("mar", "mar_ratio"))

    win_rate = _comparison_value(perf, ("win_rate", "win_rate_r"))
    profit_factor = _comparison_value(perf, ("profit_factor", "profit_factor_r"))

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    lines = [
        "bulletproof_bt Backtest Summary",
        f"Run Dir: {run_dir.name}",
        f"Generated: {generated}Z",
        "",
        "TOP METRICS",
        f"- Trades: {trades_count}",
        f"- Net PnL: {_fmt(perf.get('net_pnl', 0.0))}",
        f"- Gross PnL: {_fmt(perf.get('gross_pnl', 0.0))}",
        f"- Total Return: {_fmt(top_total_return)}",
        f"- Max Drawdown: {_fmt(top_max_drawdown)}",
        f"- Sharpe: {_fmt(sharpe)}",
        f"- Sortino: {_fmt(sortino)}",
        f"- MAR: {_fmt(mar)}",
        f"- Win Rate: {_fmt(win_rate)}",
        f"- Profit Factor: {_fmt(profit_factor)}",
        f"- EV (R) Net: {_fmt(perf.get('ev_r_net'))}",
        f"- EV (R) Gross: {_fmt(perf.get('ev_r_gross'))}",
        "",
        "WORST STREAK",
        f"- Longest Loss Streak: {'None' if longest_loss_streak is None else longest_loss_streak}",
        f"- Max Drawdown Duration: {'None' if max_drawdown_duration is None else max_drawdown_duration}",
        f"- Worst Trade R: {_fmt(worst_trade_r)}",
        "",
        "COST ATTRIBUTION",
        f"- Price PnL (from fills): {_fmt(perf.get('gross_pnl', 0.0))}",
        f"- Fees (explicit cash debits): {_fmt(perf.get('fee_total', 0.0))}",
        f"- Net PnL (price PnL - fees): {_fmt(perf.get('net_pnl', 0.0))}",
        f"- Fee Drag % vs Price PnL: {_fmt(perf.get('fee_drag_pct', 0.0))}",
        f"- Slippage Total (diagnostic, embedded in fills): {_fmt(perf.get('slippage_total', 0.0))}",
        f"- Spread Total (diagnostic, embedded in fills): {_fmt(perf.get('spread_total', 0.0))}",
        "",
        "BENCHMARK COMPARISON",
    ]

    if comparison is not None or benchmark_metrics is not None:
        lines.extend(
            [
                f"- Strategy Total Return: {_fmt(strategy_total)}",
                f"- Benchmark Total Return: {_fmt(benchmark_total)}",
                f"- Excess Return: {_fmt(excess_return)}",
            ]
        )
    else:
        lines.append("Benchmark: disabled")

    costs_payload = perf.get("costs") if isinstance(perf.get("costs"), dict) else {}
    margin_payload = perf.get("margin") if isinstance(perf.get("margin"), dict) else {}

    lines.extend(
        [
            "",
            "COST DRAG",
            f"- Spread Total: {_fmt(costs_payload.get('spread_total', perf.get('spread_total', 0.0)))}",
            f"- Slippage Total: {_fmt(costs_payload.get('slippage_total', perf.get('slippage_total', 0.0)))}",
            f"- Fees Total: {_fmt(costs_payload.get('fees_total', perf.get('fee_total', 0.0)))}",
            f"- Commission Total: {_fmt(costs_payload.get('commission_total', 0.0))}",
            "",
            "MARGIN UTILIZATION",
            f"- Peak Utilization %: {_fmt(margin_payload.get('peak_utilization_pct'))}",
            f"- Avg Utilization %: {_fmt(margin_payload.get('avg_utilization_pct'))}",
            f"- Min Free Margin %: {_fmt(margin_payload.get('min_free_margin_pct'))}",
            "",
            "MOST IMPORTANT CONCLUSION",
            derive_conclusion(perf, comparison if (comparison is not None or benchmark_metrics is not None) else None),
            "",
        ]
    )

    stop_contract = _parse_stop_contract(run_status)
    if stop_contract is not None:
        raw_counts = stop_contract.get("counts")
        counts = raw_counts if isinstance(raw_counts, dict) else {}
        version = _safe_int(stop_contract.get("version"))
        mode = str(stop_contract.get("mode", "safe"))
        allow_legacy_proxy = bool(stop_contract.get("allow_legacy_proxy", False))
        lines.extend(
            [
                f"Stop Contract (v{version if version is not None else 1})",
                f"  mode: {mode}",
                f"  legacy proxy allowed: {str(allow_legacy_proxy).lower()}",
                "  resolved: "
                f"explicit={_safe_int(counts.get('resolved_explicit')) or 0} "
                f"atr={_safe_int(counts.get('resolved_atr')) or 0} "
                f"structural={_safe_int(counts.get('resolved_structural')) or 0} "
                f"hybrid={_safe_int(counts.get('resolved_hybrid')) or 0}",
                f"  fallback legacy proxy: {_safe_int(counts.get('fallback_legacy_proxy')) or 0}",
                "  rejected: "
                f"missing_stop={_safe_int(counts.get('rejected_missing_stop')) or 0} "
                f"unresolvable={_safe_int(counts.get('rejected_unresolvable_stop')) or 0} "
                f"invalid_payload={_safe_int(counts.get('rejected_invalid_stop_payload')) or 0}",
            ]
        )
        hint = _stop_contract_hint({k: _safe_int(v) or 0 for k, v in counts.items()})
        if hint is not None:
            lines.append(f"  {hint}")
        lines.append("")

    summary_path = run_dir / "summary.txt"
    write_text_deterministic(summary_path, "\n".join(lines))
    return summary_path
