"""Experiment artifact truth validation.

This module is intentionally conservative: it validates source-of-truth run
artifacts after backtests and before downstream analysis/memory ingestion.
The checks do not change engine behavior; they fail loudly when completed
artifacts are internally inconsistent or violate configured risk limits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd


PASS_STATUSES = {"PASS", "PASSED", "COMPLETED", "completed", "done", "DONE"}
RUN_TRUTH_ARTIFACTS = (
    "config_used.yaml",
    "decisions.jsonl",
    "fills.jsonl",
    "trades.csv",
    "equity.csv",
    "performance.json",
    "run_status.json",
)


@dataclass
class TruthIssue:
    severity: str
    run_id: str
    check: str
    message: str
    value: Any | None = None


@dataclass
class TruthReport:
    experiment_root: str
    generated_at: str
    status: str
    runs_seen: int
    runs_checked: int
    hard_failures: int
    warnings: int
    issues: list[TruthIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_root": self.experiment_root,
            "generated_at": self.generated_at,
            "status": self.status,
            "runs_seen": self.runs_seen,
            "runs_checked": self.runs_checked,
            "hard_failures": self.hard_failures,
            "warnings": self.warnings,
            "issues": [issue.__dict__ for issue in self.issues],
        }


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def _num_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _bool_true_count(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0
    return int(df[column].astype(str).str.lower().isin({"true", "1", "yes"}).sum())


def _metric(perf: dict[str, Any], *names: str) -> Any | None:
    for name in names:
        if name in perf:
            return perf[name]
    return None


def _add(issues: list[TruthIssue], severity: str, run_id: str, check: str, message: str, value: Any | None = None) -> None:
    issues.append(TruthIssue(severity=severity, run_id=run_id, check=check, message=message, value=value))


def _actual_entry_notional(df: pd.DataFrame) -> pd.Series:
    qty = _num_series(df, "entry_qty")
    if qty.empty:
        qty = _num_series(df, "qty")
    price = _num_series(df, "entry_price")
    if qty.empty or price.empty:
        return pd.Series(dtype="float64")
    return qty.abs() * price.abs()


def _check_source_ts(df: pd.DataFrame, *, run_id: str, issues: list[TruthIssue]) -> None:
    decision_ts = None
    for candidate in ("entry_state_ts", "identity_ts_signal", "entry_ts", "signal_ts"):
        if candidate in df.columns:
            decision_ts = pd.to_datetime(df[candidate], utc=True, errors="coerce")
            break
    if decision_ts is None:
        _add(issues, "warning", run_id, "source_ts", "No decision timestamp column found for source timestamp checks")
        return
    for column in ("entry_state_funding_source_ts", "entry_state_funding_available_at", "entry_state_oi_source_ts", "entry_state_oi_available_at"):
        if column not in df.columns:
            continue
        source_ts = pd.to_datetime(df[column], utc=True, errors="coerce")
        leaked = source_ts.notna() & decision_ts.notna() & (source_ts > decision_ts)
        if leaked.any():
            _add(
                issues,
                "error",
                run_id,
                "no_lookahead_source_ts",
                f"{column} is greater than the decision timestamp",
                {"rows": int(leaked.sum()), "max_delta_seconds": float((source_ts[leaked] - decision_ts[leaked]).dt.total_seconds().max())},
            )


def _check_metrics(df: pd.DataFrame, perf: dict[str, Any], *, run_id: str, issues: list[TruthIssue]) -> None:
    total = _metric(perf, "total_trades", "num_trades", "trades")
    if total is not None and int(total) != len(df):
        _add(issues, "error", run_id, "trade_count", "performance trade count does not match trades.csv rows", {"performance": total, "trades_csv": len(df)})

    r = _num_series(df, "r_multiple_net")
    if r.empty:
        r = _num_series(df, "realized_r_net")
    if r.empty:
        r = _num_series(df, "r_net")
    if not r.empty:
        wins = int((r > 0).sum())
        win_rate = wins / len(df) if len(df) else 0.0
        perf_wr = _metric(perf, "win_rate", "win_rate_pct")
        if perf_wr is not None:
            perf_wr = float(perf_wr)
            if perf_wr > 1.0:
                perf_wr /= 100.0
            if abs(perf_wr - win_rate) > 0.002:
                _add(issues, "error", run_id, "win_rate", "performance win rate does not match trade outcomes", {"performance": perf_wr, "computed": win_rate, "wins": wins})

        for key in ("ev_r_net", "avg_r_net", "mean_r_net"):
            if key in perf and abs(float(perf[key]) - float(r.mean())) > 0.002:
                _add(issues, "error", run_id, "ev_r_net", f"{key} does not match mean net R from trades.csv", {"performance": perf[key], "computed": float(r.mean())})
                break

    risk_amount = _num_series(df, "risk_amount")
    pnl_net = _num_series(df, "pnl_net")
    if not risk_amount.empty and not pnl_net.empty and not r.empty:
        mask = risk_amount.abs() > 1e-12
        if mask.any():
            reconstructed = pnl_net[mask] / risk_amount[mask]
            drift = (reconstructed - r[mask]).abs()
            bad = drift > 0.02
            if bad.any():
                _add(issues, "error", run_id, "r_multiple_net", "net R does not reconcile with pnl_net / risk_amount", {"rows": int(bad.sum()), "max_abs_drift": float(drift.max())})


def _check_risk(df: pd.DataFrame, *, run_id: str, issues: list[TruthIssue], notional_tolerance_pct: float) -> None:
    actual_notional = _actual_entry_notional(df)
    max_notional = _num_series(df, "max_notional")
    if not actual_notional.empty and not max_notional.empty:
        mask = max_notional.notna() & (max_notional > 0)
        if mask.any():
            allowed = max_notional[mask] * (1.0 + notional_tolerance_pct)
            excess = actual_notional[mask] - allowed
            bad = excess > 1e-8
            if bad.any():
                _add(
                    issues,
                    "error",
                    run_id,
                    "entry_notional_cap",
                    "actual filled entry notional exceeds max_notional",
                    {"rows": int(bad.sum()), "max_excess": float(excess[bad].max()), "max_ratio": float((actual_notional[mask] / max_notional[mask]).max())},
                )

    equity = _num_series(df, "equity_used")
    if not actual_notional.empty and not equity.empty:
        pct = actual_notional / equity.replace(0.0, pd.NA)
        cap_pct = _num_series(df, "max_notional")
        # The explicit max_notional check above is the source of truth; this
        # derived pct is kept in the report path via max_notional/equity.
        if pct.dropna().gt(1.0).any():
            _add(issues, "error", run_id, "notional_pct_equity", "entry notional exceeds account equity", {"max_pct": float(pct.max())})

    free_margin = _num_series(df, "free_margin_post")
    if not free_margin.empty and (free_margin < -1e-8).any():
        _add(issues, "error", run_id, "free_margin_post", "free_margin_post became negative", {"min": float(free_margin.min())})

    if "r_metrics_valid" in df.columns:
        invalid = df["r_metrics_valid"].astype(str).str.lower().isin({"false", "0", "nan", "none"})
        if invalid.any():
            _add(issues, "error", run_id, "r_metrics_valid", "trades contain invalid R metrics", {"rows": int(invalid.sum())})

    forced = _bool_true_count(df, "forced_liquidation")
    if forced:
        _add(issues, "error", run_id, "forced_liquidation", "trades include forced liquidation events", {"rows": forced})

    risk_amount = _num_series(df, "risk_amount")
    risk_budget = _num_series(df, "risk_budget")
    stop_distance = _num_series(df, "entry_stop_distance")
    if stop_distance.empty:
        stop_distance = _num_series(df, "stop_distance")
    qty = _num_series(df, "entry_qty")
    if qty.empty:
        qty = _num_series(df, "qty")
    multiplier = _num_series(df, "risk_value_per_price_unit")
    if multiplier.empty:
        multiplier = pd.Series(1.0, index=df.index)
    if risk_amount.empty or risk_budget.empty or stop_distance.empty or qty.empty:
        _add(
            issues,
            "error",
            run_id,
            "risk_truth_fields",
            "risk_amount, risk_budget, entry stop distance, and entry quantity are required",
        )
    else:
        expected_risk = qty.abs() * stop_distance.abs() * multiplier.fillna(1.0)
        valid = expected_risk.notna() & risk_amount.notna() & (expected_risk > 0)
        if valid.any():
            drift = (risk_amount[valid] - expected_risk[valid]).abs()
            tolerance = expected_risk[valid].abs().mul(1e-6).clip(lower=1e-8)
            bad = drift > tolerance
            if bad.any():
                _add(
                    issues,
                    "error",
                    run_id,
                    "actual_stop_risk",
                    "risk_amount does not equal filled quantity times frozen stop risk",
                    {"rows": int(bad.sum()), "max_abs_drift": float(drift[bad].max())},
                )
        over_budget = risk_amount.notna() & risk_budget.notna() & (risk_amount > risk_budget * (1.0 + 1e-6))
        if over_budget.any():
            _add(issues, "error", run_id, "risk_budget", "actual stop risk exceeds requested risk budget", {"rows": int(over_budget.sum())})


def _check_schema(df: pd.DataFrame, *, run_id: str, issues: list[TruthIssue]) -> None:
    required = {
        "entry_ts",
        "exit_ts",
        "symbol",
        "side",
        "entry_price",
        "exit_price",
        "pnl_net",
        "risk_amount",
        "risk_budget",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        _add(issues, "error", run_id, "required_columns", "trades.csv missing required columns", missing)
    rich_any = any(column.startswith("entry_state_") for column in df.columns)
    if not rich_any:
        _add(issues, "warning", run_id, "rich_state", "No entry_state_* columns found; run may be OHLCV-only or enrichment failed")
    if "entry_state_csi_source" in df.columns:
        sources = sorted(df["entry_state_csi_source"].dropna().astype(str).unique().tolist())
        if not sources:
            _add(issues, "warning", run_id, "csi_source", "entry_state_csi_source exists but is empty")

    # pandas suffixes repeated CSV headers with .1. Identical legacy aliases
    # are harmless, but divergent truth fields must stop downstream ingestion.
    for duplicate in [column for column in df.columns if column.endswith(".1")]:
        original = duplicate[:-2]
        if original not in df.columns:
            continue
        left = pd.to_numeric(df[original], errors="coerce")
        right = pd.to_numeric(df[duplicate], errors="coerce")
        comparable = left.notna() | right.notna()
        tolerance = pd.concat([left.abs(), right.abs()], axis=1).max(axis=1).mul(1e-12).clip(lower=1e-12)
        mismatch = comparable & ((left - right).abs() > tolerance)
        mismatch |= left.notna() ^ right.notna()
        if mismatch.any():
            _add(
                issues,
                "error",
                run_id,
                "duplicate_truth_column",
                f"Duplicate columns {original} and {duplicate} diverge",
                {"rows": int(mismatch.sum())},
            )


def _check_extracted_dataset(experiment_root: Path, issues: list[TruthIssue]) -> None:
    dataset_path = experiment_root / "research_data" / "trades_dataset.parquet"
    if not dataset_path.exists():
        return
    try:
        trades = pd.read_parquet(dataset_path)
    except Exception as exc:
        _add(issues, "error", "<experiment>", "trades_dataset", f"Unable to read extracted trades dataset: {exc}")
        return
    required = ("trade_id", "identity_trade_id", "parameter_set_id", "identity_parameter_set_id", "identity_ts_signal", "net_pnl", "pnl_r")
    for column in required:
        if column not in trades.columns:
            _add(issues, "error", "<experiment>", "extracted_required_column", f"Extracted dataset missing {column}")
            continue
        blank = trades[column].isna() | trades[column].astype(str).str.strip().isin({"", "nan", "None"})
        if blank.any():
            _add(issues, "error", "<experiment>", "extracted_null_truth", f"Extracted dataset has null {column}", {"rows": int(blank.sum())})
    if "identity_trade_id" in trades.columns and trades["identity_trade_id"].duplicated().any():
        _add(issues, "error", "<experiment>", "extracted_duplicate_trade_id", "Extracted identity_trade_id values are not unique")
    _check_source_ts(trades, run_id="<extracted_dataset>", issues=issues)


def validate_experiment_root(
    experiment_root: Path,
    *,
    allow_incomplete: bool = False,
    notional_tolerance_pct: float = 0.005,
) -> TruthReport:
    issues: list[TruthIssue] = []
    runs_dir = experiment_root / "runs"
    run_dirs = sorted(path for path in runs_dir.glob("row_*") if path.is_dir()) if runs_dir.exists() else []
    runs_checked = 0

    if not run_dirs:
        _add(issues, "error", "<experiment>", "runs_dir", f"No run directories found under {runs_dir}")

    for run_dir in run_dirs:
        run_id = run_dir.name
        status_path = run_dir / "run_status.json"
        perf_path = run_dir / "performance.json"
        trades_path = run_dir / "trades.csv"
        equity_path = run_dir / "equity.csv"

        if not status_path.exists():
            _add(issues, "error", run_id, "run_status", "Missing run_status.json")
            continue
        status = _read_json(status_path)
        run_status = str(status.get("status", ""))
        if run_status not in PASS_STATUSES:
            severity = "warning" if allow_incomplete else "error"
            _add(issues, severity, run_id, "run_status", "Run status is not completed/PASS", run_status)
            if allow_incomplete:
                continue
        for artifact_name in RUN_TRUTH_ARTIFACTS:
            artifact = run_dir / artifact_name
            if not artifact.exists():
                _add(issues, "error", run_id, "required_artifact", f"Missing required artifact: {artifact.name}")
        if not perf_path.exists() or not trades_path.exists():
            continue

        perf = _read_json(perf_path)
        try:
            trades = pd.read_csv(trades_path, low_memory=False)
        except Exception as exc:
            _add(issues, "error", run_id, "trades_csv", f"Unable to read trades.csv: {exc}")
            continue
        runs_checked += 1
        _check_schema(trades, run_id=run_id, issues=issues)
        _check_metrics(trades, perf, run_id=run_id, issues=issues)
        _check_risk(trades, run_id=run_id, issues=issues, notional_tolerance_pct=notional_tolerance_pct)
        _check_source_ts(trades, run_id=run_id, issues=issues)

    _check_extracted_dataset(experiment_root, issues)

    hard = sum(1 for issue in issues if issue.severity == "error")
    warnings = sum(1 for issue in issues if issue.severity == "warning")
    return TruthReport(
        experiment_root=str(experiment_root),
        generated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        status="PASS" if hard == 0 else "FAIL",
        runs_seen=len(run_dirs),
        runs_checked=runs_checked,
        hard_failures=hard,
        warnings=warnings,
        issues=issues,
    )


def write_truth_report(report: TruthReport, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "truth_validation_report.json"
    md_path = output_dir / "truth_validation_report.md"
    json_path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Truth Validation Report",
        "",
        f"- Status: `{report.status}`",
        f"- Experiment root: `{report.experiment_root}`",
        f"- Runs seen: `{report.runs_seen}`",
        f"- Runs checked: `{report.runs_checked}`",
        f"- Hard failures: `{report.hard_failures}`",
        f"- Warnings: `{report.warnings}`",
        "",
    ]
    if report.issues:
        lines.append("## Issues")
        lines.append("")
        for issue in report.issues:
            lines.append(f"- `{issue.severity}` `{issue.run_id}` `{issue.check}`: {issue.message}")
            if issue.value is not None:
                lines.append(f"  - value: `{issue.value}`")
    else:
        lines.append("No issues found.")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path
