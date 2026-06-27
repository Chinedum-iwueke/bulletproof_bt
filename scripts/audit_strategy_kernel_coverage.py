#!/usr/bin/env python3
"""Audit fast-path kernel coverage for hypothesis YAMLs.

This is intentionally a coverage audit, not a parity substitute. It verifies
that every hypothesis resolves to a strategy that is routed onto a real
compiled-control-flow path under the research-panel fast mode. Per-family
signal parity is still enforced by ``scripts/compare_all_stable_fast_paths.py``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from bt.engine.fast_path.signal_compiler import inspect_support
from bt.engine.fast_path.family_kernels import kernel_for_strategy
from bt.hypotheses.contract import HypothesisContract


def _timeframes(contract: HypothesisContract) -> list[str]:
    values = {
        str(spec.get("params", {}).get("signal_timeframe", spec.get("params", {}).get("timeframe", ""))).lower()
        for spec in contract.to_run_specs()
    }
    return sorted(value for value in values if value)


def _row(path: Path, *, universe: str) -> dict[str, Any]:
    contract = HypothesisContract.from_yaml(path)
    strategy = str(contract.schema.entry.get("strategy", ""))
    timeframes = _timeframes(contract)
    data: dict[str, Any] = {
        "dataset_kind": "research_panel",
        "universe": universe,
        "timeframe": "1m",
    }
    if universe == "stable":
        data["htf_context_source"] = "precomputed"
    config = {
        "execution_engine": "auto",
        "data": data,
        "htf_resampler": {"timeframes": timeframes or ["15m"], "strict": True},
        "strategy": {"name": strategy},
    }
    support = inspect_support(config)
    kernel = kernel_for_strategy(strategy)
    return {
        "hypothesis": path.name,
        "strategy": strategy,
        "universe": universe,
        "signal_timeframes": ",".join(timeframes),
        "kernel_mode": kernel.mode if kernel is not None else "",
        "kernel_name": kernel.kernel_name if kernel is not None else "",
        "numerical_signal_kernel": bool(kernel.numerical_signal_kernel) if kernel is not None else False,
        "supported": bool(support.supported and kernel is not None),
        "reason": support.reason,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hypotheses-dir", default="research/hypotheses")
    parser.add_argument("--output-dir", default="research/audits")
    parser.add_argument("--universes", default="stable,volatile")
    parser.add_argument("--fail-on-missing", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    hypotheses = [
        path
        for path in sorted(Path(args.hypotheses_dir).glob("*.yaml"))
        if path.name != "sample_pipeline_smoke.yaml"
    ]
    universes = [item.strip().lower() for item in args.universes.split(",") if item.strip()]
    rows = [_row(path, universe=universe) for path in hypotheses for universe in universes]
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "strategy_kernel_coverage.csv"
    json_path = out_dir / "strategy_kernel_coverage.json"
    md_path = out_dir / "STRATEGY_KERNEL_COVERAGE.md"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    json_path.write_text(json.dumps({"rows": rows}, indent=2, sort_keys=True), encoding="utf-8")

    missing = [row for row in rows if not row["supported"]]
    lines = [
        "# Strategy Kernel Coverage",
        "",
        f"hypotheses: {len(hypotheses)}",
        f"rows: {len(rows)}",
        f"unsupported: {len(missing)}",
        "",
        "| hypothesis | universe | strategy | kernel_mode | numerical_signal_kernel | supported |",
        "|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['hypothesis']} | {row['universe']} | {row['strategy']} | "
            f"{row['kernel_mode']} | {row['numerical_signal_kernel']} | {row['supported']} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")
    print(f"wrote {md_path}")
    if missing and args.fail_on_missing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
