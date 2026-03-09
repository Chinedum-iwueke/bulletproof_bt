#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# helpers
# -----------------------------
def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def extract_param_from_run_name(run_name: str, key: str) -> Optional[float]:
    """
    Extract things like:
      run_001__adx25_vol70_er045_n16
    key in {"adx", "vol", "er", "n"}
    """
    m = re.search(rf"{key}(\d+)", run_name)
    if not m:
        return None
    v = m.group(1)
    if key == "er":
        # er045 -> 0.45
        return float(v) / 100.0
    return float(v)


def first_existing(df: pd.DataFrame, cols: List[str]) -> Optional[str]:
    for c in cols:
        if c in df.columns:
            return c
    return None


def coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def empirical_ccdf(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    s = coerce_numeric(series).dropna().sort_values()
    if s.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    n = len(s)
    ccdf = pd.Series([(n - i) / n for i in range(n)], index=s.index)
    return s.reset_index(drop=True), ccdf.reset_index(drop=True)


def summarize_dist(series: pd.Series, prefix: str) -> Dict[str, float]:
    s = coerce_numeric(series).dropna()
    if s.empty:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_mean": math.nan,
            f"{prefix}_median": math.nan,
            f"{prefix}_p90": math.nan,
            f"{prefix}_p95": math.nan,
            f"{prefix}_p99": math.nan,
            f"{prefix}_max": math.nan,
            f"{prefix}_frac_ge_1R": math.nan,
            f"{prefix}_frac_ge_2R": math.nan,
            f"{prefix}_frac_ge_3R": math.nan,
            f"{prefix}_frac_ge_5R": math.nan,
        }
    return {
        f"{prefix}_count": int(len(s)),
        f"{prefix}_mean": float(s.mean()),
        f"{prefix}_median": float(s.median()),
        f"{prefix}_p90": float(s.quantile(0.90)),
        f"{prefix}_p95": float(s.quantile(0.95)),
        f"{prefix}_p99": float(s.quantile(0.99)),
        f"{prefix}_max": float(s.max()),
        f"{prefix}_frac_ge_1R": float((s >= 1.0).mean()),
        f"{prefix}_frac_ge_2R": float((s >= 2.0).mean()),
        f"{prefix}_frac_ge_3R": float((s >= 3.0).mean()),
        f"{prefix}_frac_ge_5R": float((s >= 5.0).mean()),
    }


# -----------------------------
# MFE extraction logic
# -----------------------------
def detect_realized_r(df: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str]]:
    """
    Try to detect realized return already expressed in R.
    Preferred columns are explicit R-based realized trade columns.
    """
    candidates = [
        "realized_r",
        "r_realized",
        "r_multiple",
        "r_mult",
        "r",
        "trade_r",
        "net_r",
        "pnl_r",
    ]
    col = first_existing(df, candidates)
    if col:
        return coerce_numeric(df[col]), col
    return None, None


def detect_mfe_r_direct(df: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str]]:
    """
    Preferred if engine already stores MFE in R.
    """
    candidates = [
        "mfe_r",
        "max_favorable_excursion_r",
        "mfe_multiple_r",
        "mfe_multiple",
    ]
    col = first_existing(df, candidates)
    if col:
        return coerce_numeric(df[col]), col
    return None, None


def detect_mae_r_direct(df: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[str]]:
    candidates = [
        "mae_r",
        "max_adverse_excursion_r",
        "mae_multiple_r",
        "mae_multiple",
    ]
    col = first_existing(df, candidates)
    if col:
        return coerce_numeric(df[col]), col
    return None, None


def compute_mfe_mae_from_prices(df: pd.DataFrame) -> Tuple[Optional[pd.Series], Optional[pd.Series], str]:
    """
    Compute MFE_R and MAE_R from price-path extrema and stop distance if possible.

    For longs:
      mfe_price = max_price - entry
      mae_price = entry - min_price

    For shorts:
      mfe_price = entry - min_price
      mae_price = max_price - entry

    Then divide by stop_distance_price.

    We try common column names for:
      side / direction
      entry price
      max/min during trade
      stop price or stop distance
    """
    entry_col = first_existing(df, ["entry_price", "entry", "avg_entry_price"])
    side_col = first_existing(df, ["side", "direction", "position_side"])
    max_col = first_existing(df, ["max_price_during_trade", "trade_high", "max_price", "highest_price", "high_during_trade"])
    min_col = first_existing(df, ["min_price_during_trade", "trade_low", "min_price", "lowest_price", "low_during_trade"])

    stop_dist_col = first_existing(df, ["stop_distance_price", "initial_stop_distance", "stop_dist_price"])
    stop_col = first_existing(df, ["initial_stop_price", "stop_price", "initial_stop"])

    if entry_col is None or side_col is None or max_col is None or min_col is None:
        return None, None, "missing_entry_side_or_trade_extrema"

    entry = coerce_numeric(df[entry_col])
    maxp = coerce_numeric(df[max_col])
    minp = coerce_numeric(df[min_col])

    if stop_dist_col:
        stop_dist = coerce_numeric(df[stop_dist_col]).abs()
    elif stop_col:
        stopp = coerce_numeric(df[stop_col])
        stop_dist = (entry - stopp).abs()
    else:
        return None, None, "missing_stop_distance"

    side = df[side_col].astype(str).str.lower().str.strip()

    is_long = side.isin(["long", "buy", "1", "true"])
    is_short = side.isin(["short", "sell", "-1", "false"])

    mfe_price = pd.Series(index=df.index, dtype=float)
    mae_price = pd.Series(index=df.index, dtype=float)

    mfe_price.loc[is_long] = maxp.loc[is_long] - entry.loc[is_long]
    mae_price.loc[is_long] = entry.loc[is_long] - minp.loc[is_long]

    mfe_price.loc[is_short] = entry.loc[is_short] - minp.loc[is_short]
    mae_price.loc[is_short] = maxp.loc[is_short] - entry.loc[is_short]

    mfe_r = mfe_price / stop_dist
    mae_r = mae_price / stop_dist

    return mfe_r, mae_r, "computed_from_trade_extrema"


def capture_ratio(realized_r: pd.Series, mfe_r: pd.Series) -> pd.Series:
    """
    realized_r / mfe_r for trades where mfe_r > 0.
    Clip only by dropping impossible values; let >1 be visible if data odd.
    """
    rr = coerce_numeric(realized_r)
    mr = coerce_numeric(mfe_r)
    out = rr / mr
    out = out.where(mr > 0)
    return out.replace([float("inf"), -float("inf")], math.nan)


# -----------------------------
# plots
# -----------------------------
def plot_ccdf(series: pd.Series, title: str, outpath: Path, xlabel: str) -> None:
    s, ccdf = empirical_ccdf(series)
    if s.empty:
        return
    plt.figure(figsize=(7, 5))
    plt.plot(s, ccdf)
    plt.yscale("log")
    plt.xlabel(xlabel)
    plt.ylabel("CCDF: P(X >= x)")
    plt.title(title)
    plt.grid(True, linestyle="--", linewidth=0.5)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()


def plot_capture_hist(series: pd.Series, title: str, outpath: Path) -> None:
    s = coerce_numeric(series).dropna()
    if s.empty:
        return
    plt.figure(figsize=(7, 5))
    plt.hist(s, bins=50)
    plt.xlabel("Capture ratio = realized_R / MFE_R")
    plt.ylabel("Trade count")
    plt.title(title)
    plt.grid(True, linestyle="--", linewidth=0.5)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()


# -----------------------------
# run analysis
# -----------------------------
def analyze_run(run_dir: Path) -> Dict[str, object]:
    trades = read_csv_safe(run_dir / "trades.csv")
    if trades.empty:
        return {
            "run_id": run_dir.name,
            "run_dir": str(run_dir),
            "status": "missing_trades_csv",
        }

    realized_r, realized_src = detect_realized_r(trades)
    mfe_r, mfe_direct_src = detect_mfe_r_direct(trades)
    mae_r, mae_direct_src = detect_mae_r_direct(trades)

    source = {}
    if mfe_r is None or mae_r is None:
        mfe_r2, mae_r2, method = compute_mfe_mae_from_prices(trades)
        if mfe_r is None:
            mfe_r = mfe_r2
        if mae_r is None:
            mae_r = mae_r2
        source["mfe_mae_method"] = method
    else:
        source["mfe_mae_method"] = "direct_r_columns"

    if realized_r is None:
        return {
            "run_id": run_dir.name,
            "run_dir": str(run_dir),
            "status": "missing_realized_r",
            "mfe_mae_method": source.get("mfe_mae_method", "unknown"),
        }

    if mfe_r is None:
        return {
            "run_id": run_dir.name,
            "run_dir": str(run_dir),
            "status": "missing_mfe_r",
            "realized_r_source": realized_src,
            "mfe_mae_method": source.get("mfe_mae_method", "unknown"),
        }

    cap = capture_ratio(realized_r, mfe_r)

    row: Dict[str, object] = {
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "status": "ok",
        "realized_r_source": realized_src,
        "mfe_r_source": mfe_direct_src or source.get("mfe_mae_method"),
        "mae_r_source": mae_direct_src or source.get("mfe_mae_method"),
        "vol_floor": extract_param_from_run_name(run_dir.name, "vol"),
        "adx_min": extract_param_from_run_name(run_dir.name, "adx"),
        "er_min": extract_param_from_run_name(run_dir.name, "er"),
        "er_lookback": extract_param_from_run_name(run_dir.name, "n"),
    }

    row.update(summarize_dist(realized_r, "realized_r"))
    row.update(summarize_dist(mfe_r, "mfe_r"))
    if mae_r is not None:
        row.update(summarize_dist(mae_r, "mae_r"))
    row.update(summarize_dist(cap, "capture_ratio"))

    # Salvage heuristics
    p95_mfe = row.get("mfe_r_p95")
    p99_mfe = row.get("mfe_r_p99")
    frac3 = row.get("mfe_r_frac_ge_3R")
    cap_med = row.get("capture_ratio_median")

    salvage_flag = False
    salvage_reason = "no_hidden_tail"

    if (
        pd.notna(p95_mfe)
        and pd.notna(p99_mfe)
        and pd.notna(frac3)
        and pd.notna(cap_med)
    ):
        if p95_mfe >= 2.0 and p99_mfe >= 5.0 and frac3 >= 0.01 and cap_med < 0.5:
            salvage_flag = True
            salvage_reason = "strong_hidden_tail_and_low_capture"
        elif p95_mfe >= 1.5 and p99_mfe >= 3.0 and frac3 >= 0.005 and cap_med < 0.4:
            salvage_flag = True
            salvage_reason = "moderate_hidden_tail_and_low_capture"
        elif p95_mfe >= 2.0 and cap_med < 0.3:
            salvage_flag = True
            salvage_reason = "large_mfe_tail_but_exit_captures_little"

    row["chandelier_salvage_candidate"] = salvage_flag
    row["chandelier_salvage_reason"] = salvage_reason

    return row


# -----------------------------
# main
# -----------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose whether runs are salvageable with a better exit using MFE/MAE.")
    parser.add_argument("--roots", nargs="*", default=[], help="Roots containing run_* folders")
    parser.add_argument("--runs", nargs="*", default=[], help="Explicit run directories")
    parser.add_argument("--glob", default="run_*", help="Glob pattern under each root")
    parser.add_argument("--outdir", required=True, help="Output directory for diagnostics")
    parser.add_argument("--top", type=int, default=15, help="Top N rows for candidate csv")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    run_dirs: List[Path] = []
    if args.runs:
        run_dirs = [Path(x) for x in args.runs]
    else:
        roots = [Path(x) for x in args.roots]
        for root in roots:
            if not root.exists():
                continue
            run_dirs.extend([p for p in root.glob(args.glob) if p.is_dir()])

    run_dirs = sorted(set(run_dirs))
    if not run_dirs:
        print("No run directories found.")
        return 1

    rows: List[Dict[str, object]] = []
    ccdf_dir = outdir / "ccdf"
    capture_dir = outdir / "capture"

    for run_dir in run_dirs:
        row = analyze_run(run_dir)
        rows.append(row)

        if row.get("status") != "ok":
            continue

        trades = read_csv_safe(run_dir / "trades.csv")
        realized_r, _ = detect_realized_r(trades)
        mfe_r, _ = detect_mfe_r_direct(trades)
        mae_r, _ = detect_mae_r_direct(trades)
        if mfe_r is None or mae_r is None:
            mfe_r, mae_r, _ = compute_mfe_mae_from_prices(trades)

        if realized_r is not None and mfe_r is not None:
            cap = capture_ratio(realized_r, mfe_r)
            plot_ccdf(
                mfe_r,
                title=f"{run_dir.name} | MFE_R CCDF",
                outpath=ccdf_dir / f"{run_dir.name}__mfe_ccdf.png",
                xlabel="MFE in R",
            )
            plot_ccdf(
                realized_r,
                title=f"{run_dir.name} | Realized_R CCDF",
                outpath=ccdf_dir / f"{run_dir.name}__realized_ccdf.png",
                xlabel="Realized R",
            )
            plot_capture_hist(
                cap,
                title=f"{run_dir.name} | Capture ratio",
                outpath=capture_dir / f"{run_dir.name}__capture_hist.png",
            )

    df = pd.DataFrame(rows)
    df.to_csv(outdir / "mfe_diagnostics.csv", index=False)

    if not df.empty:
        if "chandelier_salvage_candidate" in df.columns:
            cand = df[df["chandelier_salvage_candidate"] == True].copy()
            sort_cols = [c for c in ["mfe_r_p99", "mfe_r_p95", "mfe_r_frac_ge_3R", "capture_ratio_median"] if c in cand.columns]
            if not cand.empty and sort_cols:
                cand = cand.sort_values(
                    sort_cols,
                    ascending=[False, False, False, True][:len(sort_cols)],
                )
        else:
            cand = pd.DataFrame()

        cand.to_csv(outdir / "mfe_salvage_candidates.csv", index=False)

        summary_cols = [
            "run_id",
            "status",
            "vol_floor",
            "adx_min",
            "er_min",
            "er_lookback",
            "realized_r_mean",
            "realized_r_p95",
            "realized_r_p99",
            "mfe_r_mean",
            "mfe_r_p95",
            "mfe_r_p99",
            "mfe_r_frac_ge_2R",
            "mfe_r_frac_ge_3R",
            "capture_ratio_median",
            "capture_ratio_p95",
            "chandelier_salvage_candidate",
            "chandelier_salvage_reason",
        ]
        existing_summary_cols = [c for c in summary_cols if c in df.columns]

        if existing_summary_cols:
            sort_cols = [c for c in ["chandelier_salvage_candidate", "mfe_r_p99", "mfe_r_p95"] if c in df.columns]
            if sort_cols:
                df[existing_summary_cols].sort_values(
                    sort_cols,
                    ascending=[False, False, False][:len(sort_cols)],
                ).to_csv(outdir / "mfe_brief.csv", index=False)
            else:
                df[existing_summary_cols].to_csv(outdir / "mfe_brief.csv", index=False)
        else:
            pd.DataFrame().to_csv(outdir / "mfe_brief.csv", index=False)

    print(f"Wrote: {outdir / 'mfe_diagnostics.csv'}")
    print(f"Wrote: {outdir / 'mfe_salvage_candidates.csv'}")
    print(f"Wrote: {outdir / 'mfe_brief.csv'}")
    print(f"Plots: {ccdf_dir}/ and {capture_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
