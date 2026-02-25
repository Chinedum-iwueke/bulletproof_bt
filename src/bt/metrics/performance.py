"""Performance metrics computation for backtest runs."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import yaml

from bt.metrics.r_metrics import summarize_r
from bt.logging.formatting import FLOAT_DECIMALS_CSV, write_json_deterministic
from bt.contracts.schema_versions import PERFORMANCE_SCHEMA_VERSION
from bt.logging.cost_breakdown import write_cost_breakdown_json


@dataclass(frozen=True)
class PerformanceReport:
    run_id: str
    initial_equity: float
    final_equity: float
    total_return: float
    total_trades: int
    ev_net: float
    ev_gross: float
    win_rate: float
    max_drawdown_pct: float
    max_drawdown_duration_bars: int
    tail_loss_p95: float
    tail_loss_p99: float
    fee_total: float
    slippage_total: float
    spread_total: float
    gross_pnl: float
    net_pnl: float
    fee_drag_pct: float
    slippage_drag_pct: float
    spread_drag_pct: float
    fee_drag_pct_of_gross: Optional[float]
    slippage_drag_pct_of_gross: Optional[float]
    trade_return_skew: Optional[float]
    trade_return_kurtosis_excess: Optional[float]
    worst_streak_loss: float
    max_consecutive_losses: int
    sharpe_annualized: Optional[float]
    sortino_annualized: Optional[float]
    cagr: Optional[float]
    mar_ratio: Optional[float]
    ev_by_bucket: Dict[str, float]
    trades_by_bucket: Dict[str, int]
    ev_r_gross: Optional[float]
    ev_r_net: Optional[float]
    win_rate_r: Optional[float]
    avg_r_win: Optional[float]
    avg_r_loss: Optional[float]
    profit_factor_r: Optional[float]
    payoff_ratio_r: Optional[float]
    extra: Dict[str, Any]


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _sum_costs(df: pd.DataFrame, *, preferred: str, fallbacks: list[str]) -> pd.Series:
    if preferred in df.columns:
        return _coerce_numeric(df[preferred])
    available = [col for col in fallbacks if col in df.columns]
    if not available:
        return pd.Series(0.0, index=df.index)
    if len(available) == 1:
        return _coerce_numeric(df[available[0]])
    return _coerce_numeric(df[available]).sum(axis=1)


def _drag_pct(cost: float, gross_pnl: float) -> float:
    denominator = abs(float(gross_pnl))
    if denominator == 0.0:
        return 0.0
    return 100.0 * float(cost) / denominator


def _read_fills_cost_totals(fills_path: Path) -> tuple[float, float, float]:
    fee_total = 0.0
    slippage_total = 0.0
    spread_total = 0.0

    with fills_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                continue
            fee_value = record.get("fee", record.get("fee_paid", 0.0))
            slippage_value = record.get("slippage", record.get("slippage_cost", 0.0))
            metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
            spread_value = record.get("spread_cost", metadata.get("spread_cost", 0.0))

            fee_total += abs(float(fee_value or 0.0))
            slippage_total += abs(float(slippage_value or 0.0))
            spread_total += abs(float(spread_value or 0.0))

    return float(fee_total), float(slippage_total), float(spread_total)


def compute_cost_attribution(
    run_dir: Path,
    *,
    fills_path: Path | None = None,
    trades_path: Path | None = None,
) -> dict[str, float]:
    """Returns deterministic run-level PnL/cost attribution fields."""
    resolved_trades_path = trades_path if trades_path is not None else run_dir / "trades.csv"
    resolved_fills_path = fills_path if fills_path is not None else run_dir / "fills.jsonl"

    if not resolved_trades_path.exists():
        raise ValueError(
            f"run_dir={run_dir}: missing fills/trades required for cost attribution (missing trades.csv)"
        )

    try:
        trades_df = pd.read_csv(resolved_trades_path)
    except pd.errors.EmptyDataError:
        trades_df = pd.DataFrame()

    if trades_df.empty:
        return {
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "fee_total": 0.0,
            "slippage_total": 0.0,
            "spread_total": 0.0,
        }

    fills_fee_total = 0.0
    fills_slippage_total = 0.0
    fills_spread_total = 0.0
    if resolved_fills_path.exists():
        fills_fee_total, fills_slippage_total, fills_spread_total = _read_fills_cost_totals(resolved_fills_path)

    if "fees_paid" in trades_df.columns:
        fees_series = _coerce_numeric(trades_df["fees_paid"])
        fee_total = float(fees_series.sum())
    else:
        fees_series = _sum_costs(trades_df, preferred="fees_total", fallbacks=["fees", "fee"])
        fee_total = float(abs(fees_series.sum()))
        if fee_total == 0.0 and fills_fee_total > 0.0:
            fee_total = float(fills_fee_total)

    if "pnl_price" in trades_df.columns:
        pnl_price_series = _coerce_numeric(trades_df["pnl_price"])
    elif "pnl" in trades_df.columns:
        pnl_price_series = _coerce_numeric(trades_df["pnl"])
    elif "pnl_net" in trades_df.columns:
        pnl_net_series = _coerce_numeric(trades_df["pnl_net"])
        if fee_total > 0.0:
            pnl_price_series = pnl_net_series.copy()
            pnl_price_series.iloc[-1] = pnl_price_series.iloc[-1] + fee_total
        else:
            pnl_price_series = pnl_net_series + fees_series.abs()
    else:
        raise ValueError(f"run_dir={run_dir}: trades.csv must include pnl_price/pnl or pnl_net")

    pnl_price = float(pnl_price_series.sum())

    if "pnl_net" in trades_df.columns:
        net_pnl = float(_coerce_numeric(trades_df["pnl_net"]).sum())
    else:
        net_pnl = float(pnl_price - fee_total)

    if resolved_fills_path.exists():
        slippage_total = float(fills_slippage_total)
        spread_total = float(fills_spread_total)
    else:
        slippage = _sum_costs(
            trades_df, preferred="slippage_total", fallbacks=["slippage", "slip"]
        )
        slippage_total = float(abs(slippage.sum()))
        spread_total = 0.0

    gross_pnl = float(pnl_price)
    return {
        "gross_pnl": gross_pnl,
        "net_pnl": float(net_pnl),
        "fee_total": float(fee_total),
        "slippage_total": float(slippage_total),
        "spread_total": float(spread_total),
    }




def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator_numeric = _coerce_numeric(denominator)
    numerator_numeric = _coerce_numeric(numerator)
    ratio = pd.Series(1.0, index=denominator_numeric.index, dtype=float)
    positive = denominator_numeric > 0
    ratio.loc[positive] = numerator_numeric.loc[positive] / denominator_numeric.loc[positive]
    return ratio


def _compute_margin_summary(equity_df: pd.DataFrame) -> dict[str, float]:
    if equity_df.empty or "equity" not in equity_df.columns:
        return {
            "peak_used_margin": 0.0,
            "avg_used_margin": 0.0,
            "peak_utilization_pct": 0.0,
            "avg_utilization_pct": 0.0,
            "min_free_margin": 0.0,
            "min_free_margin_pct": 0.0,
        }

    used_margin = _coerce_numeric(equity_df["used_margin"]) if "used_margin" in equity_df.columns else pd.Series(0.0, index=equity_df.index)
    free_margin = _coerce_numeric(equity_df["free_margin"]) if "free_margin" in equity_df.columns else pd.Series(0.0, index=equity_df.index)
    equity = _coerce_numeric(equity_df["equity"])

    utilization_pct = _safe_ratio(used_margin, equity)
    free_margin_pct = _safe_ratio(free_margin, equity)

    return {
        "peak_used_margin": float(used_margin.max()) if not used_margin.empty else 0.0,
        "avg_used_margin": float(used_margin.mean()) if not used_margin.empty else 0.0,
        "peak_utilization_pct": float(utilization_pct.max()) if not utilization_pct.empty else 0.0,
        "avg_utilization_pct": float(utilization_pct.mean()) if not utilization_pct.empty else 0.0,
        "min_free_margin": float(free_margin.min()) if not free_margin.empty else 0.0,
        "min_free_margin_pct": float(free_margin_pct.min()) if not free_margin_pct.empty else 0.0,
    }

def _max_drawdown_duration(dd: pd.Series) -> int:
    if dd.empty:
        return 0
    underwater = dd < 0
    max_len = 0
    current = 0
    for flag in underwater:
        if flag:
            current += 1
            max_len = max(max_len, current)
        else:
            current = 0
    return max_len


def _append_note(extra: Dict[str, Any], note: str) -> None:
    notes = extra.setdefault("notes", [])
    if isinstance(notes, list):
        notes.append(note)
    else:
        extra["notes"] = [note]


def _bucket_metrics(
    pnl_net: pd.Series, bucket_series: Optional[pd.Series]
) -> tuple[Dict[str, float], Dict[str, int]]:
    if pnl_net.empty:
        return {"all": 0.0}, {"all": 0}
    if bucket_series is None:
        return {"all": float(pnl_net.mean())}, {"all": int(pnl_net.shape[0])}
    buckets = bucket_series.fillna("unknown").astype(str)
    grouped = pnl_net.groupby(buckets)
    ev = grouped.mean().sort_index()
    counts = grouped.size().sort_index()
    ev_by_bucket = {str(idx): float(val) for idx, val in ev.items()}
    trades_by_bucket = {str(idx): int(val) for idx, val in counts.items()}
    return ev_by_bucket, trades_by_bucket


def _infer_periods_per_year(ts_series: pd.Series) -> int:
    if ts_series.empty:
        return 365
    ts = pd.to_datetime(ts_series, errors="coerce").dropna()
    if ts.shape[0] < 2:
        return 365
    deltas = ts.sort_values().diff().dropna().dt.total_seconds()
    if deltas.empty:
        return 365
    median_seconds = float(deltas.median())
    if median_seconds <= 90:
        return 365 * 24 * 60
    return 365


def _load_periods_per_year(run_path: Path, ts_series: pd.Series) -> int:
    config_path = run_path / "config_used.yaml"
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
        periods = config.get("periods_per_year")
        if periods is not None:
            try:
                return int(periods)
            except (TypeError, ValueError):
                return _infer_periods_per_year(ts_series)
    return _infer_periods_per_year(ts_series)


def _compute_trade_returns(
    trades_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    columns = [
        "entry_ts",
        "exit_ts",
        "symbol",
        "side",
        "trade_return",
        "pnl",
        "fees",
        "slippage",
    ]
    if trades_df.empty:
        empty_df = pd.DataFrame(columns=columns)
        return empty_df, pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)

    pnl_col = "pnl_net" if "pnl_net" in trades_df.columns else "pnl"
    if pnl_col not in trades_df.columns:
        raise ValueError("Trades must include pnl_net or pnl column")

    pnl_net = _coerce_numeric(trades_df[pnl_col])
    fees = _sum_costs(trades_df, preferred="fees_total", fallbacks=["fees", "fee"])
    slippage = _sum_costs(
        trades_df, preferred="slippage_total", fallbacks=["slippage", "slip"]
    )
    entry_price = _coerce_numeric(
        trades_df.get("entry_price", pd.Series(0.0, index=trades_df.index))
    )
    qty = _coerce_numeric(trades_df.get("qty", pd.Series(0.0, index=trades_df.index)))
    # TODO: support equity-based / margin-based denominators.
    entry_notional = (entry_price * qty).abs()
    trade_return = pnl_net.where(entry_notional != 0, np.nan) / entry_notional.replace(
        0, np.nan
    )

    trade_returns_df = pd.DataFrame(
        {
            "entry_ts": trades_df.get(
                "entry_ts", pd.Series("", index=trades_df.index)
            ),
            "exit_ts": trades_df.get("exit_ts", pd.Series("", index=trades_df.index)),
            "symbol": trades_df.get("symbol", pd.Series("", index=trades_df.index)),
            "side": trades_df.get("side", pd.Series("", index=trades_df.index)),
            "trade_return": trade_return,
            "pnl": pnl_net,
            "fees": fees,
            "slippage": slippage,
        }
    )
    return trade_returns_df, trade_return, fees, slippage


def _order_trade_returns(trade_returns_df: pd.DataFrame) -> pd.DataFrame:
    if trade_returns_df.empty:
        return trade_returns_df
    ordered = trade_returns_df.copy()
    ordered["_entry_ts_sort"] = pd.to_datetime(
        ordered["entry_ts"], errors="coerce"
    )
    ordered["_symbol_sort"] = ordered["symbol"].fillna("").astype(str)
    ordered = ordered.sort_values(
        by=["_entry_ts_sort", "_symbol_sort"],
        kind="mergesort",
        na_position="last",
    )
    return ordered.drop(columns=["_entry_ts_sort", "_symbol_sort"])


def _trade_return_moments(
    trade_returns: pd.Series, extra: Dict[str, Any]
) -> tuple[Optional[float], Optional[float]]:
    values = trade_returns.dropna().to_numpy(dtype=float)
    if values.size < 3:
        _append_note(
            extra,
            "trade_return_distribution: insufficient trades for skew/kurtosis",
        )
        return None, None
    mean = float(np.mean(values))
    std = float(np.std(values))
    if std == 0:
        _append_note(
            extra,
            "trade_return_distribution: zero variance for skew/kurtosis",
        )
        return None, None
    centered = values - mean
    skew = float(np.mean(centered**3) / (std**3))
    kurtosis_excess = float(np.mean(centered**4) / (std**4) - 3.0)
    return skew, kurtosis_excess


def _trade_return_streaks(trade_returns: pd.Series) -> tuple[int, float]:
    max_consecutive_losses = 0
    worst_streak_loss = 0.0
    current_losses = 0
    current_sum = 0.0
    for value in trade_returns.dropna().to_numpy(dtype=float):
        if value < 0:
            current_losses += 1
            current_sum += value
            max_consecutive_losses = max(max_consecutive_losses, current_losses)
            worst_streak_loss = min(worst_streak_loss, current_sum)
        else:
            current_losses = 0
            current_sum = 0.0
    return max_consecutive_losses, worst_streak_loss


def compute_param_stability(run_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    if len(run_summaries) < 3:
        return {
            "ev_std_across_params": None,
            "dd_std_across_params": None,
            "sharpe_surface_flatness": None,
            "notes": ["insufficient runs for param stability metrics"],
        }

    ev_values = [summary.get("ev_net") for summary in run_summaries]
    dd_values = [summary.get("max_drawdown_pct") for summary in run_summaries]
    sharpe_values = [summary.get("sharpe_annualized") for summary in run_summaries]

    def _to_float(values: list[Any]) -> list[float]:
        return [float(value) for value in values if value is not None]

    ev_float = _to_float(ev_values)
    dd_float = _to_float(dd_values)
    sharpe_float = _to_float(sharpe_values)

    if len(ev_float) < 3 or len(dd_float) < 3 or len(sharpe_float) < 3:
        return {
            "ev_std_across_params": None,
            "dd_std_across_params": None,
            "sharpe_surface_flatness": None,
            "notes": ["insufficient data for param stability metrics"],
        }

    ev_std = float(np.std(ev_float))
    dd_std = float(np.std(dd_float))
    sharpe_std = float(np.std(sharpe_float))
    sharpe_mean = float(np.mean(sharpe_float))
    flatness = sharpe_std / (abs(sharpe_mean) + 1e-12)
    return {
        "ev_std_across_params": ev_std,
        "dd_std_across_params": dd_std,
        "sharpe_surface_flatness": flatness,
        "notes": [],
    }


def compute_performance(run_dir: str | Path) -> PerformanceReport:
    """
    Load run_dir/equity.csv and run_dir/trades.csv, compute required metrics,
    and return a PerformanceReport.
    """
    run_path = Path(run_dir)
    run_id = run_path.name

    equity_path = run_path / "equity.csv"
    trades_path = run_path / "trades.csv"

    equity_df = pd.read_csv(equity_path)
    trades_df = pd.read_csv(trades_path)
    cost_attribution = compute_cost_attribution(
        run_path,
        fills_path=(run_path / "fills.jsonl"),
        trades_path=trades_path,
    )

    equity_col = None
    for candidate in ["equity", "total_equity", "portfolio_equity"]:
        if candidate in equity_df.columns:
            equity_col = candidate
            break
    if equity_col is None:
        raise ValueError("Equity column not found in equity.csv")

    equity_series = _coerce_numeric(equity_df[equity_col])
    initial_equity = float(equity_series.iloc[0]) if not equity_series.empty else 0.0
    final_equity = float(equity_series.iloc[-1]) if not equity_series.empty else 0.0
    total_return = 0.0
    if initial_equity != 0.0:
        total_return = (final_equity / initial_equity) - 1.0

    peak = equity_series.cummax()
    dd = equity_series / peak - 1.0 if not equity_series.empty else equity_series
    max_drawdown_pct = float(dd.min()) if not dd.empty else 0.0
    max_drawdown_duration_bars = _max_drawdown_duration(dd)

    extra: Dict[str, Any] = {}

    returns = equity_series.pct_change().dropna()
    sharpe_annualized: Optional[float]
    sortino_annualized: Optional[float]
    if returns.shape[0] < 3:
        sharpe_annualized = None
        sortino_annualized = None
    else:
        periods_per_year = _load_periods_per_year(
            run_path, equity_df.get("ts", pd.Series(index=equity_df.index))
        )
        mean_return = float(returns.mean())
        std_return = float(returns.std(ddof=0))
        sharpe_annualized = (
            mean_return / std_return * float(np.sqrt(periods_per_year))
            if std_return != 0
            else None
        )
        downside = returns[returns < 0]
        downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
        sortino_annualized = (
            mean_return / downside_std * float(np.sqrt(periods_per_year))
            if downside_std != 0
            else None
        )

    cagr: Optional[float]
    mar_ratio: Optional[float]
    if "ts" not in equity_df.columns:
        cagr = None
        mar_ratio = None
    else:
        ts_values = pd.to_datetime(equity_df["ts"], errors="coerce").dropna()
        if ts_values.shape[0] < 2 or equity_series.empty:
            cagr = None
            mar_ratio = None
        else:
            total_seconds = float((ts_values.iloc[-1] - ts_values.iloc[0]).total_seconds())
            seconds_per_year = 365.25243600 * 24 * 60 * 60
            total_years = total_seconds / seconds_per_year if seconds_per_year > 0 else 0.0
            if total_years <= 0 or initial_equity <= 0 or final_equity <= 0:
                cagr = None
            else:
                cagr = float((final_equity / initial_equity) ** (1.0 / total_years) - 1.0)
            if cagr is None or max_drawdown_pct == 0:
                mar_ratio = None
            else:
                mar_ratio = cagr / abs(max_drawdown_pct)

    total_trades = int(trades_df.shape[0])
    if total_trades == 0:
        _append_note(
            extra,
            "trade_return_distribution: insufficient trades for skew/kurtosis",
        )
        return PerformanceReport(
            run_id=run_id,
            initial_equity=initial_equity,
            final_equity=final_equity,
            total_return=total_return,
            total_trades=0,
            ev_net=0.0,
            ev_gross=0.0,
            win_rate=0.0,
            max_drawdown_pct=max_drawdown_pct,
            max_drawdown_duration_bars=max_drawdown_duration_bars,
            tail_loss_p95=0.0,
            tail_loss_p99=0.0,
            fee_total=cost_attribution["fee_total"],
            slippage_total=cost_attribution["slippage_total"],
            spread_total=cost_attribution["spread_total"],
            gross_pnl=cost_attribution["gross_pnl"],
            net_pnl=cost_attribution["net_pnl"],
            fee_drag_pct=_drag_pct(cost_attribution["fee_total"], cost_attribution["gross_pnl"]),
            slippage_drag_pct=_drag_pct(cost_attribution["slippage_total"], cost_attribution["gross_pnl"]),
            spread_drag_pct=_drag_pct(cost_attribution["spread_total"], cost_attribution["gross_pnl"]),
            fee_drag_pct_of_gross=None,
            slippage_drag_pct_of_gross=None,
            trade_return_skew=None,
            trade_return_kurtosis_excess=None,
            worst_streak_loss=0.0,
            max_consecutive_losses=0,
            sharpe_annualized=sharpe_annualized,
            sortino_annualized=sortino_annualized,
            cagr=cagr,
            mar_ratio=mar_ratio,
            ev_by_bucket={"all": 0.0},
            trades_by_bucket={"all": 0},
            ev_r_gross=None,
            ev_r_net=None,
            win_rate_r=None,
            avg_r_win=None,
            avg_r_loss=None,
            profit_factor_r=None,
            payoff_ratio_r=None,
            extra=extra,
        )

    if "fees_paid" in trades_df.columns:
        fees = _coerce_numeric(trades_df["fees_paid"])
    else:
        fees = _sum_costs(trades_df, preferred="fees_total", fallbacks=["fees", "fee"])

    if "pnl_price" in trades_df.columns:
        pnl_price = _coerce_numeric(trades_df["pnl_price"])
    elif "pnl" in trades_df.columns:
        pnl_price = _coerce_numeric(trades_df["pnl"])
    elif "pnl_net" in trades_df.columns:
        pnl_price = _coerce_numeric(trades_df["pnl_net"]) + fees.abs()
    else:
        raise ValueError("Trades must include pnl_price/pnl or pnl_net column")

    if "pnl_net" in trades_df.columns:
        pnl_net = _coerce_numeric(trades_df["pnl_net"])
    else:
        pnl_net = pnl_price - fees

    ev_net = float(pnl_net.mean())
    ev_gross = float(pnl_price.mean())
    win_rate = float((pnl_net > 0).mean())

    loss_values = -pnl_net[pnl_net < 0]
    if loss_values.empty:
        tail_loss_p95 = 0.0
        tail_loss_p99 = 0.0
    else:
        tail_loss_p95 = float(loss_values.quantile(0.95))
        tail_loss_p99 = float(loss_values.quantile(0.99))

    fee_total = cost_attribution["fee_total"]
    slippage_total = cost_attribution["slippage_total"]
    spread_total = cost_attribution["spread_total"]
    gross_pnl = cost_attribution["gross_pnl"]
    net_pnl = cost_attribution["net_pnl"]

    fee_drag_pct = _drag_pct(fee_total, gross_pnl)
    slippage_drag_pct = _drag_pct(slippage_total, gross_pnl)
    spread_drag_pct = _drag_pct(spread_total, gross_pnl)

    if gross_pnl != 0:
        fee_drag_pct_of_gross = fee_total / gross_pnl
        slippage_drag_pct_of_gross = slippage_total / gross_pnl
    else:
        fee_drag_pct_of_gross = None
        slippage_drag_pct_of_gross = None

    bucket_series = None
    if "vol_bucket" in trades_df.columns:
        bucket_series = trades_df["vol_bucket"]
    elif "regime_bucket" in trades_df.columns:
        bucket_series = trades_df["regime_bucket"]

    ev_by_bucket, trades_by_bucket = _bucket_metrics(pnl_net, bucket_series)

    r_net_summary = summarize_r(trades_df.get("r_multiple_net", pd.Series(index=trades_df.index)))
    r_gross_summary = summarize_r(
        trades_df.get("r_multiple_gross", pd.Series(index=trades_df.index))
    )

    trade_returns_df, trade_returns, _, _ = _compute_trade_returns(trades_df)
    trade_returns = trade_returns.dropna()
    trade_return_skew, trade_return_kurtosis_excess = _trade_return_moments(
        trade_returns, extra
    )
    max_consecutive_losses, worst_streak_loss = _trade_return_streaks(trade_returns_df["trade_return"])

    return PerformanceReport(
        run_id=run_id,
        initial_equity=initial_equity,
        final_equity=final_equity,
        total_return=total_return,
        total_trades=total_trades,
        ev_net=ev_net,
        ev_gross=ev_gross,
        win_rate=win_rate,
        max_drawdown_pct=max_drawdown_pct,
        max_drawdown_duration_bars=max_drawdown_duration_bars,
        tail_loss_p95=tail_loss_p95,
        tail_loss_p99=tail_loss_p99,
        fee_total=fee_total,
        slippage_total=slippage_total,
        spread_total=spread_total,
        gross_pnl=gross_pnl,
        net_pnl=net_pnl,
        fee_drag_pct=fee_drag_pct,
        slippage_drag_pct=slippage_drag_pct,
        spread_drag_pct=spread_drag_pct,
        fee_drag_pct_of_gross=fee_drag_pct_of_gross,
        slippage_drag_pct_of_gross=slippage_drag_pct_of_gross,
        trade_return_skew=trade_return_skew,
        trade_return_kurtosis_excess=trade_return_kurtosis_excess,
        worst_streak_loss=worst_streak_loss,
        max_consecutive_losses=max_consecutive_losses,
        sharpe_annualized=sharpe_annualized,
        sortino_annualized=sortino_annualized,
        cagr=cagr,
        mar_ratio=mar_ratio,
        ev_by_bucket=ev_by_bucket,
        trades_by_bucket=trades_by_bucket,
        ev_r_gross=r_gross_summary.ev_r,
        ev_r_net=r_net_summary.ev_r,
        win_rate_r=r_net_summary.win_rate,
        avg_r_win=r_net_summary.avg_r_win,
        avg_r_loss=r_net_summary.avg_r_loss,
        profit_factor_r=r_net_summary.profit_factor_r,
        payoff_ratio_r=r_net_summary.payoff_ratio_r,
        extra=extra,
    )


def write_performance_artifacts(report: PerformanceReport, run_dir: str | Path) -> None:
    """
    Write:
      - run_dir/performance.json
      - run_dir/performance_by_bucket.csv  (bucket, trades, ev_net)
    Deterministic ordering.
    """
    run_path = Path(run_dir)
    performance_path = run_path / "performance.json"
    by_bucket_path = run_path / "performance_by_bucket.csv"
    trade_returns_path = run_path / "trade_returns.csv"

    performance_payload = asdict(report)
    performance_payload["schema_version"] = PERFORMANCE_SCHEMA_VERSION
    performance_payload["costs"] = {
        "fees_total": float(performance_payload.get("fee_total", 0.0)),
        "slippage_total": float(performance_payload.get("slippage_total", 0.0)),
        "spread_total": float(performance_payload.get("spread_total", 0.0)),
        "commission_total": 0.0,
    }

    equity_path = run_path / "equity.csv"
    try:
        equity_df = pd.read_csv(equity_path)
    except pd.errors.EmptyDataError:
        equity_df = pd.DataFrame()
    performance_payload["margin"] = _compute_margin_summary(equity_df)

    write_json_deterministic(performance_path, performance_payload)
    write_cost_breakdown_json(run_path, performance_payload)

    rows = []
    for bucket in sorted(report.ev_by_bucket.keys()):
        rows.append(
            {
                "bucket": bucket,
                "n_trades": report.trades_by_bucket.get(bucket, 0),
                "ev_net": report.ev_by_bucket[bucket],
            }
        )
    pd.DataFrame(rows, columns=["bucket", "n_trades", "ev_net"]).to_csv(
        by_bucket_path, index=False, float_format=f"%.{FLOAT_DECIMALS_CSV}f"
    )

    trades_path = run_path / "trades.csv"
    if trades_path.exists():
        try:
            trades_df = pd.read_csv(trades_path)
        except pd.errors.EmptyDataError:
            trades_df = pd.DataFrame()
    else:
        trades_df = pd.DataFrame()
    trade_returns_df, _, _, _ = _compute_trade_returns(trades_df)
    ordered_trade_returns = _order_trade_returns(trade_returns_df)
    ordered_trade_returns.to_csv(
        trade_returns_path,
        index=False,
        float_format=f"%.{FLOAT_DECIMALS_CSV}f",
    )

    param_sweep_dir = run_path / "param_sweep"
    if param_sweep_dir.exists() and param_sweep_dir.is_dir():
        run_summaries = []
        for subdir in sorted(param_sweep_dir.iterdir()):
            if not subdir.is_dir():
                continue
            perf_path = subdir / "performance.json"
            if not perf_path.exists():
                continue
            with perf_path.open("r", encoding="utf-8") as handle:
                try:
                    run_summaries.append(json.load(handle))
                except json.JSONDecodeError:
                    continue
        if run_summaries:
            stability = compute_param_stability(run_summaries)
            stability_path = run_path / "param_stability.json"
            write_json_deterministic(stability_path, stability)
