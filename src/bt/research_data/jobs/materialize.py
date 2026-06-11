"""Materialize research-data views used by fast, causal backtests."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from bt.data.parquet_io import ensure_pyarrow_parquet
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts


MATERIALIZED_VOLATILE_SYMBOL = "_volatile_active"

MATERIALIZED_RESEARCH_PANEL_COLUMNS = (
    "ts",
    "exchange",
    "symbol",
    "canonical_symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "mark_close",
    "index_close",
    "funding_rate",
    "funding_source_ts",
    "open_interest",
    "oi_source_ts",
    "oi_change_1",
    "oi_change_pct_1",
    "premium_mark_vs_index",
    "basis_close_vs_index",
    "liq_buy_notional",
    "liq_sell_notional",
    "volatile_active",
    "universe_active",
)


def materialized_volatile_panel_path(
    store: ResearchDataStore,
    exchange: str,
    timeframe: str = "1m",
) -> Path:
    return store.canonical_path(exchange, MATERIALIZED_VOLATILE_SYMBOL, timeframe, "research_panel")


def materialize_volatile_panel(
    exchange: str,
    timeframe: str = "1m",
    *,
    membership_path: str | Path | None = None,
    start: object | None = None,
    end: object | None = "now",
    store: ResearchDataStore | None = None,
    row_group_size: int = 120_000,
) -> Path:
    """Build a single ts/symbol sorted panel containing active volatile rows only.

    The volatile membership file is the source of truth. This job does not rank
    or choose symbols; it only applies historical membership intervals to the
    already-built canonical symbol panels, then atomically replaces the
    materialized fast-path parquet file.
    """
    ensure_pyarrow_parquet()
    store = store or ResearchDataStore()
    membership_file = Path(membership_path) if membership_path else store.manifest_path("volatile_universe_membership")
    if not membership_file.exists():
        raise FileNotFoundError(f"Volatile membership manifest missing: {membership_file}")
    membership = pd.read_parquet(membership_file)
    membership = _prepare_membership(membership, exchange=exchange, start=start, end=end)
    output_path = materialized_volatile_panel_path(store, exchange, timeframe)
    if membership.empty:
        empty = pd.DataFrame(columns=list(MATERIALIZED_RESEARCH_PANEL_COLUMNS))
        store.write_atomic(empty, output_path)
        return output_path

    intervals = _membership_intervals(membership, end=end)
    frames: list[pd.DataFrame] = []
    for symbol, symbol_intervals in intervals.groupby("symbol", sort=True):
        panel_path = store.canonical_path(exchange, str(symbol), timeframe, "research_panel")
        if not panel_path.exists():
            continue
        panel = pd.read_parquet(panel_path)
        if panel.empty:
            continue
        panel = _prepare_panel(panel, exchange=exchange, symbol=str(symbol), start=start, end=end)
        active = _filter_active_rows(panel, symbol_intervals)
        if active.empty:
            continue
        active["volatile_active"] = True
        active["universe_active"] = True
        frames.append(_select_materialized_columns(active))

    if frames:
        materialized = pd.concat(frames, ignore_index=True)
        materialized = materialized.drop_duplicates(["ts", "symbol"], keep="last")
        materialized = materialized.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)
    else:
        materialized = pd.DataFrame(columns=list(MATERIALIZED_RESEARCH_PANEL_COLUMNS))

    _write_materialized_parquet(store, materialized, output_path, row_group_size=row_group_size)
    return output_path


def _prepare_membership(
    membership: pd.DataFrame,
    *,
    exchange: str,
    start: object | None,
    end: object | None,
) -> pd.DataFrame:
    required = {"ts", "exchange", "symbol"}
    missing = required - set(membership.columns)
    if missing:
        raise ValueError(f"volatile membership missing columns: {sorted(missing)}")
    out = membership.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    out["symbol"] = out["symbol"].astype(str)
    out = out[out["exchange"].astype(str).eq(exchange)]
    if start is not None:
        start_ts = utc_ts(start)
        before = out[out["ts"].le(start_ts)]
        anchor_ts = before["ts"].max() if not before.empty else None
        mask = out["ts"].ge(start_ts)
        if anchor_ts is not None and pd.notna(anchor_ts):
            mask |= out["ts"].eq(anchor_ts)
        out = out.loc[mask]
    if end is not None:
        out = out[out["ts"].lt(utc_ts(end))]
    return out.drop_duplicates(["exchange", "symbol", "ts"], keep="last").sort_values(["ts", "symbol"])


def _membership_intervals(membership: pd.DataFrame, *, end: object | None) -> pd.DataFrame:
    rebalance_ts = pd.Series(sorted(membership["ts"].drop_duplicates()))
    if rebalance_ts.empty:
        return pd.DataFrame(columns=["symbol", "start_ts", "end_ts"])
    next_by_ts = dict(zip(rebalance_ts.iloc[:-1], rebalance_ts.iloc[1:]))
    cap_end = utc_ts(end) if end is not None else _infer_open_interval_end(rebalance_ts)
    rows: list[dict[str, object]] = []
    for row in membership.itertuples(index=False):
        start_ts = pd.Timestamp(row.ts)
        interval_end = next_by_ts.get(start_ts, cap_end)
        if pd.isna(interval_end) or interval_end <= start_ts:
            continue
        rows.append({"symbol": str(row.symbol), "start_ts": start_ts, "end_ts": pd.Timestamp(interval_end)})
    return pd.DataFrame(rows)


def _infer_open_interval_end(rebalance_ts: pd.Series) -> pd.Timestamp:
    if len(rebalance_ts) < 2:
        return pd.Timestamp(rebalance_ts.iloc[-1]) + pd.Timedelta(hours=2)
    diffs = rebalance_ts.diff().dropna()
    step = diffs.median()
    if pd.isna(step) or step <= pd.Timedelta(0):
        step = pd.Timedelta(hours=2)
    return pd.Timestamp(rebalance_ts.iloc[-1]) + step


def _prepare_panel(
    panel: pd.DataFrame,
    *,
    exchange: str,
    symbol: str,
    start: object | None,
    end: object | None,
) -> pd.DataFrame:
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"research panel for {exchange}/{symbol} missing columns: {sorted(missing)}")
    out = panel.copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    if "exchange" not in out.columns:
        out["exchange"] = exchange
    if "symbol" not in out.columns:
        out["symbol"] = symbol
    out["symbol"] = out["symbol"].astype(str)
    out = out[out["symbol"].eq(symbol)]
    if start is not None:
        out = out[out["ts"].ge(utc_ts(start))]
    if end is not None:
        out = out[out["ts"].lt(utc_ts(end))]
    return out.sort_values("ts", kind="mergesort").reset_index(drop=True)


def _filter_active_rows(panel: pd.DataFrame, intervals: pd.DataFrame) -> pd.DataFrame:
    if panel.empty or intervals.empty:
        return panel.iloc[0:0].copy()
    ts_ns = panel["ts"].astype("int64").to_numpy()
    mask = np.zeros(len(panel), dtype=bool)
    for row in intervals.itertuples(index=False):
        start_ns = pd.Timestamp(row.start_ts).value
        end_ns = pd.Timestamp(row.end_ts).value
        left = np.searchsorted(ts_ns, start_ns, side="left")
        right = np.searchsorted(ts_ns, end_ns, side="left")
        if right > left:
            mask[left:right] = True
    return panel.loc[mask].copy()


def _select_materialized_columns(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [col for col in MATERIALIZED_RESEARCH_PANEL_COLUMNS if col in frame.columns]
    columns.extend(
        col
        for col in frame.columns
        if col.startswith("entry_state_") and col not in columns
    )
    return frame.loc[:, columns]


def _write_materialized_parquet(
    store: ResearchDataStore,
    frame: pd.DataFrame,
    path: Path,
    *,
    row_group_size: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with store.write_lock():
        if frame.empty:
            store.write_atomic(frame, path)
            return
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp = Path(handle.name)
        try:
            frame.to_parquet(tmp, index=False, row_group_size=row_group_size)
            with open(tmp, "rb") as handle:
                try:
                    os.fsync(handle.fileno())
                except OSError:
                    pass
            os.replace(tmp, path)
            try:
                dir_fd = os.open(path.parent, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        finally:
            if tmp.exists():
                tmp.unlink()


__all__ = [
    "MATERIALIZED_VOLATILE_SYMBOL",
    "materialized_volatile_panel_path",
    "materialize_volatile_panel",
]
