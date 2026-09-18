"""Point-in-time held-out evaluation for governed alpha research."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bt.governance.research_bridge import BridgeError
from bt.institutional.receipt import digest


def held_out_trade_evaluation(run_dir: Path, test_start: str) -> dict[str, Any]:
    """Score only held-out trades and apply a second copy of observed costs."""
    trades_path = run_dir / "trades.csv"
    try:
        trades = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades = pd.DataFrame()
    if trades.empty:
        return {
            "test_start": test_start,
            "trade_count": 0,
            "mean_net_r": 0.0,
            "double_cost_mean_net_r": 0.0,
            "adequate_support": False,
            "positive_net_edge": False,
            "cost_stress_passed": False,
        }
    entry = pd.to_datetime(trades["entry_ts"], utc=True, errors="coerce")
    sample = trades.loc[entry >= pd.Timestamp(test_start)]
    net_column = "r_net" if "r_net" in sample else "r_multiple_net"
    cost_column = "cost_drag_r" if "cost_drag_r" in sample else None
    net = pd.to_numeric(sample[net_column], errors="coerce").dropna()
    costs = (
        pd.to_numeric(sample.loc[net.index, cost_column], errors="coerce").fillna(0.0)
        if cost_column
        else pd.Series(0.0, index=net.index)
    )
    stressed = net - costs.abs()
    return {
        "test_start": test_start,
        "trade_count": int(len(net)),
        "mean_net_r": float(net.mean()) if len(net) else 0.0,
        "double_cost_mean_net_r": float(stressed.mean()) if len(stressed) else 0.0,
        "adequate_support": len(net) >= 50,
        "positive_net_edge": bool(len(net) and net.mean() > 0),
        "cost_stress_passed": bool(len(stressed) and stressed.mean() > 0),
    }


def complete_five_minute_bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Build strict left-labeled 5m bars from complete, unique 1m observations."""
    required = {"ts", "symbol", "close", "quote_volume"}
    missing = required - set(frame.columns)
    if missing:
        raise BridgeError(
            f"impact-proxy evaluation is missing source fields: {sorted(missing)}"
        )
    ordered = frame.loc[:, sorted(required)].copy()
    ordered["ts"] = pd.to_datetime(ordered["ts"], utc=True, errors="raise")
    ordered = ordered.sort_values(["symbol", "ts"])
    if ordered.duplicated(["symbol", "ts"]).any():
        raise BridgeError("impact-proxy evaluation rejects duplicate minute bars")
    if (ordered["ts"].dt.second != 0).any() or (
        ordered["ts"].dt.microsecond != 0
    ).any():
        raise BridgeError("impact-proxy evaluation requires minute-aligned source bars")
    ordered["bucket"] = ordered["ts"].dt.floor("5min")
    grouped = ordered.groupby(["symbol", "bucket"], sort=True)
    complete = grouped.filter(
        lambda sample: len(sample) == 5
        and sample["ts"].nunique() == 5
        and sample["ts"].max() - sample["ts"].min() == pd.Timedelta(minutes=4)
    )
    if complete.empty:
        return pd.DataFrame(columns=["symbol", "ts", "close", "quote_volume"])
    return (
        complete.groupby(["symbol", "bucket"], sort=True)
        .agg(close=("close", "last"), quote_volume=("quote_volume", "sum"))
        .reset_index()
        .rename(columns={"bucket": "ts"})
    )


def impact_proxy_evaluation(
    frame: pd.DataFrame,
    *,
    test_start: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate held-out impact extremes against preregistered matched shocks."""
    bars = complete_five_minute_bars(frame)
    if bars.empty:
        raise BridgeError("impact-proxy evaluation has no complete 5m bars")
    threshold = float(params["impact_proxy_threshold"])
    window = int(params["normalization_window"])
    band = float(params["return_shock_control_band"])
    parts: list[pd.DataFrame] = []
    for _, sample in bars.groupby("symbol", sort=False):
        sample = sample.sort_values("ts").copy()
        sample["signal_return"] = sample["close"].pct_change()
        sample["impact_proxy"] = sample["signal_return"].abs() / sample["quote_volume"]
        sample["threshold_value"] = (
            sample["impact_proxy"]
            .shift(1)
            .rolling(window=window, min_periods=window)
            .quantile(threshold)
        )
        sample["next_30m_return"] = sample["close"].shift(-6) / sample["close"] - 1.0
        sample["signed_reversal"] = (
            -sample["signal_return"].apply(lambda value: 1.0 if value > 0 else -1.0)
            * sample["next_30m_return"]
        )
        parts.append(sample)
    evaluated = pd.concat(parts, ignore_index=True)
    evaluated = evaluated.loc[
        (evaluated["ts"] >= pd.Timestamp(test_start))
        & evaluated["signal_return"].notna()
        & evaluated["next_30m_return"].notna()
        & evaluated["threshold_value"].notna()
        & (evaluated["quote_volume"] >= 1_000_000.0)
    ].copy()
    extreme = evaluated.loc[evaluated["impact_proxy"] >= evaluated["threshold_value"]]
    controls: list[float] = []
    for row in extreme.itertuples(index=False):
        magnitude = abs(float(row.signal_return))
        low, high = magnitude * (1.0 - band), magnitude * (1.0 + band)
        candidates = evaluated.loc[
            (evaluated["symbol"] == row.symbol)
            & (evaluated["ts"] != row.ts)
            & (evaluated["impact_proxy"] < evaluated["threshold_value"])
            & (evaluated["signal_return"].abs().between(low, high))
            & ((evaluated["signal_return"] > 0) == (row.signal_return > 0))
        ]
        if not candidates.empty:
            distance = (candidates["signal_return"].abs() - magnitude).abs()
            controls.append(float(candidates.loc[distance.idxmin(), "signed_reversal"]))
    extreme_reversal = pd.to_numeric(extreme["signed_reversal"], errors="coerce").dropna()
    control_mean = float(pd.Series(controls, dtype=float).mean()) if controls else 0.0
    extreme_mean = float(extreme_reversal.mean()) if len(extreme_reversal) else 0.0
    matched = {
        "extreme_observations": int(len(extreme_reversal)),
        "matched_control_observations": len(controls),
        "extreme_mean_signed_30m_return": extreme_mean,
        "control_mean_signed_30m_return": control_mean,
        "extreme_minus_control": extreme_mean - control_mean,
        "outperformed_control": bool(controls and extreme_mean > control_mean),
    }
    report = {
        "schema_version": "alpha-impact-proxy-evaluation-v1.0.0",
        "measurement": "held-out causal predictive association; not executable PnL",
        "test_start": pd.Timestamp(test_start).isoformat(),
        "resampling": "strict complete left-labeled 5m bars from unique 1m rows",
        "parameters": {
            "impact_proxy_threshold": threshold,
            "normalization_window": window,
            "return_shock_control_band": band,
        },
        "direction_balance": {
            "long": int((extreme["signal_return"] < 0).sum()),
            "short": int((extreme["signal_return"] > 0).sum()),
        },
        "matched_return_shock_control": matched,
    }
    report["record_digest"] = digest(report)
    return report
