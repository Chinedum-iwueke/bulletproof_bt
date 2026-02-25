"""Per-symbol metrics artifact generation from trades.csv."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bt.logging.formatting import write_json_deterministic

PER_SYMBOL_SCHEMA_VERSION = 1


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


def _profit_factor(pnl_net: pd.Series) -> float | None:
    gross_win = float(pnl_net[pnl_net > 0].sum())
    gross_loss = float(abs(pnl_net[pnl_net < 0].sum()))
    if gross_loss == 0.0:
        return None
    return gross_win / gross_loss


def _per_symbol_payload(symbol: str, trades_df: pd.DataFrame) -> dict[str, Any]:
    fees = _sum_costs(trades_df, preferred="fees_paid", fallbacks=["fees_total", "fees", "fee"])
    slippage = _sum_costs(trades_df, preferred="slippage_total", fallbacks=["slippage", "slip"])

    if "pnl_net" in trades_df.columns:
        pnl_net = _coerce_numeric(trades_df["pnl_net"])
    elif "pnl" in trades_df.columns:
        pnl_net = _coerce_numeric(trades_df["pnl"]) - fees
    else:
        pnl_net = pd.Series(0.0, index=trades_df.index)

    if "pnl_price" in trades_df.columns:
        pnl_gross = _coerce_numeric(trades_df["pnl_price"])
    elif "pnl" in trades_df.columns:
        pnl_gross = _coerce_numeric(trades_df["pnl"])
    else:
        pnl_gross = pnl_net + fees

    total_trades = int(trades_df.shape[0])
    win_rate = float((pnl_net > 0).sum() / total_trades) if total_trades > 0 else 0.0

    payload: dict[str, Any] = {
        "schema_version": PER_SYMBOL_SCHEMA_VERSION,
        "symbol": symbol,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "gross_pnl": float(pnl_gross.sum()),
        "net_pnl": float(pnl_net.sum()),
        "avg_trade_pnl_net": float(pnl_net.mean()) if total_trades > 0 else 0.0,
        "median_trade_pnl_net": float(pnl_net.median()) if total_trades > 0 else 0.0,
        "best_trade_pnl_net": float(pnl_net.max()) if total_trades > 0 else 0.0,
        "worst_trade_pnl_net": float(pnl_net.min()) if total_trades > 0 else 0.0,
        "fee_total": float(fees.sum()),
        "slippage_total": float(abs(slippage).sum()),
        "profit_factor": _profit_factor(pnl_net),
    }
    return payload


def write_per_symbol_metrics(
    run_dir: str | Path,
    *,
    out_dir_name: str = "per_symbol",
) -> Path:
    """Generate per-symbol metric folders from run_dir/trades.csv."""
    run_path = Path(run_dir)
    trades_path = run_path / "trades.csv"
    if not trades_path.exists():
        raise ValueError(f"run_dir={run_path}: missing trades.csv")

    try:
        trades_df = pd.read_csv(trades_path)
    except pd.errors.EmptyDataError:
        trades_df = pd.DataFrame(columns=["symbol"])

    out_path = run_path / out_dir_name
    out_path.mkdir(parents=True, exist_ok=True)

    if trades_df.empty or "symbol" not in trades_df.columns:
        write_json_deterministic(
            out_path / "manifest.json",
            {
                "schema_version": PER_SYMBOL_SCHEMA_VERSION,
                "symbols": [],
                "total_symbols": 0,
            },
        )
        return out_path

    normalized_symbols = trades_df["symbol"].fillna("UNKNOWN").astype(str).str.strip()
    normalized_symbols = normalized_symbols.where(normalized_symbols != "", "UNKNOWN")
    working = trades_df.copy()
    working["symbol"] = normalized_symbols

    symbols_written: list[str] = []
    for symbol in sorted(working["symbol"].unique()):
        symbol_df = working[working["symbol"] == symbol].copy()
        symbol_dir = out_path / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        symbol_df.to_csv(symbol_dir / "trades.csv", index=False)
        payload = _per_symbol_payload(symbol, symbol_df)
        write_json_deterministic(symbol_dir / "metrics.json", payload)
        symbols_written.append(symbol)

    write_json_deterministic(
        out_path / "manifest.json",
        {
            "schema_version": PER_SYMBOL_SCHEMA_VERSION,
            "symbols": symbols_written,
            "total_symbols": len(symbols_written),
        },
    )
    return out_path

