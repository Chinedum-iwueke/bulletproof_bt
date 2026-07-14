"""Read-once market data snapshots for fast-path research kernels.

The snapshot layer is a data access optimization only. It does not decide
entries, exits, sizing, fills, or accounting. Classic engine semantics remain
the source of truth until a strategy-family adapter is parity-gated.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from bt.core.errors import DataError
from bt.data.parquet_io import ensure_pyarrow_parquet
from bt.data.research_panel_loader import (
    BAR_COLUMNS,
    _filter_time_range,
    _membership_intervals,
    _parse_date_range,
    _read_columns_for_panel,
    _stable_symbols,
    _volatile_membership,
    research_panel_path,
    volatile_materialized_panel_path,
)
from bt.engine.fast_path.candidate_events import candidate_columns, candidate_event_reasons
from bt.engine.fast_path.feature_registry import FeatureBank, infer_registered_specs_for_columns


@dataclass(frozen=True)
class SymbolArrays:
    symbol: str
    symbol_id: int
    ts: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    active_mask: np.ndarray
    candidate_ready: np.ndarray
    extras: Mapping[str, np.ndarray] = field(default_factory=dict)

    def __len__(self) -> int:
        return int(len(self.ts))


@dataclass(frozen=True)
class MarketDataSnapshot:
    """Columnar market snapshot keyed by integer symbol ids."""

    root: Path
    exchange: str
    universe: str
    timeframe: str
    symbols: tuple[str, ...]
    symbol_to_id: Mapping[str, int]
    arrays_by_symbol: Mapping[str, SymbolArrays]
    ts_ns: np.ndarray
    candidate_columns: tuple[str, ...]
    feature_columns: tuple[str, ...]
    feature_bank: FeatureBank
    source_paths: Mapping[str, str]
    materialized_path: str | None = None

    def arrays_for_symbol(self, symbol: str) -> SymbolArrays:
        try:
            return self.arrays_by_symbol[str(symbol)]
        except KeyError as exc:
            raise ValueError(f"symbol not present in MarketDataSnapshot: {symbol}") from exc

    def arrays_for_id(self, symbol_id: int) -> SymbolArrays:
        symbol = self.symbols[int(symbol_id)]
        return self.arrays_for_symbol(symbol)

    def active_symbols_at_ns(self, ts_ns: int) -> tuple[str, ...]:
        active: list[str] = []
        for symbol in self.symbols:
            arrays = self.arrays_by_symbol[symbol]
            idx = np.searchsorted(arrays.ts, np.int64(ts_ns))
            if idx < len(arrays.ts) and arrays.ts[idx] == ts_ns and bool(arrays.active_mask[idx]):
                active.append(symbol)
        return tuple(active)

    def to_json(self) -> dict[str, Any]:
        rows = int(sum(len(arrays) for arrays in self.arrays_by_symbol.values()))
        active_rows = int(sum(np.count_nonzero(arrays.active_mask) for arrays in self.arrays_by_symbol.values()))
        candidate_rows = int(sum(np.count_nonzero(arrays.candidate_ready) for arrays in self.arrays_by_symbol.values()))
        return {
            "schema_version": 1,
            "root": str(self.root),
            "exchange": self.exchange,
            "universe": self.universe,
            "timeframe": self.timeframe,
            "symbols": list(self.symbols),
            "symbol_count": len(self.symbols),
            "row_count": rows,
            "active_row_count": active_rows,
            "candidate_row_count": candidate_rows,
            "timestamp_count": int(len(self.ts_ns)),
            "candidate_columns": list(self.candidate_columns),
            "feature_columns": list(self.feature_columns),
            "feature_bank": self.feature_bank.to_json(),
            "materialized_path": self.materialized_path,
        }


class DataSession:
    """Load research panels once and expose contiguous NumPy market arrays."""

    def __init__(self, *, data_path: str | Path, config: dict[str, Any]) -> None:
        self.data_path = Path(data_path)
        self.config = config
        self._frame: pd.DataFrame | None = None
        self._snapshot: MarketDataSnapshot | None = None

    @classmethod
    def from_config(cls, config: dict[str, Any], *, data_path: str | Path | None = None) -> "DataSession":
        data_cfg = config.get("data") if isinstance(config.get("data"), dict) else {}
        root = data_path if data_path is not None else data_cfg.get("root", "research_data")
        return cls(data_path=root, config=config)

    def load_frame(self) -> pd.DataFrame:
        """Compatibility helper returning a read-once sorted DataFrame."""

        if self._frame is not None:
            return self._frame
        snapshot = self.snapshot()
        frames: list[pd.DataFrame] = []
        for symbol in snapshot.symbols:
            arrays = snapshot.arrays_for_symbol(symbol)
            frame = pd.DataFrame(
                {
                    "ts": pd.to_datetime(arrays.ts, utc=True),
                    "symbol": symbol,
                    "open": arrays.open,
                    "high": arrays.high,
                    "low": arrays.low,
                    "close": arrays.close,
                    "volume": arrays.volume,
                    "active": arrays.active_mask,
                    "candidate_ready": arrays.candidate_ready,
                }
            )
            for col, values in arrays.extras.items():
                frame[col] = values
            frames.append(frame)
        self._frame = pd.concat(frames, ignore_index=True).sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)
        return self._frame

    def snapshot(self) -> MarketDataSnapshot:
        if self._snapshot is not None:
            return self._snapshot
        if self.data_path.is_file() and self.data_path.suffix.lower() == ".parquet":
            self._snapshot = self._snapshot_from_single_parquet(self.data_path)
        else:
            self._snapshot = self._snapshot_from_research_data()
        return self._snapshot

    def arrays_for_symbol(self, symbol: str) -> SymbolArrays:
        return self.snapshot().arrays_for_symbol(symbol)

    def _data_config(self) -> dict[str, Any]:
        if isinstance(self.config.get("data"), dict):
            return dict(self.config["data"])
        return dict(self.config)

    def _snapshot_from_research_data(self) -> MarketDataSnapshot:
        ensure_pyarrow_parquet()
        data_cfg = self._data_config()
        root = Path(data_cfg.get("root") or self.data_path or "research_data")
        exchange = str(data_cfg.get("exchange", "binance"))
        universe = str(data_cfg.get("universe", "stable"))
        timeframe = str(data_cfg.get("timeframe", "1m"))
        start_ts, end_ts = _parse_date_range(data_cfg.get("date_range"))
        symbols_subset = _optional_symbols_from_config(data_cfg.get("symbols_subset", data_cfg.get("symbols")))
        max_symbols = _optional_int(data_cfg.get("max_symbols"))
        row_limit = _optional_int(data_cfg.get("row_limit_per_symbol"))
        extra_columns = _symbols_from_config(data_cfg.get("extra_columns"))
        extra_column_prefixes = _symbols_from_config(data_cfg.get("extra_column_prefixes"))

        membership: pd.DataFrame | None = None
        materialized_path: Path | None = None
        if universe == "stable":
            symbols = _stable_symbols(
                root=root,
                exchange=exchange,
                stable_manifest=str(data_cfg.get("stable_manifest")) if data_cfg.get("stable_manifest") else None,
                symbols_subset=symbols_subset,
                max_symbols=max_symbols,
            )
        elif universe == "volatile":
            membership_path = data_cfg.get("membership_path")
            if not membership_path:
                raise DataError("Volatile MarketDataSnapshot requires data.membership_path")
            materialized_candidate = Path(str(data_cfg.get("materialized_path") or volatile_materialized_panel_path(root, exchange, timeframe)))
            if materialized_candidate.exists():
                materialized_path = materialized_candidate
                frame = _read_panel_frame(
                    materialized_path,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    row_limit_per_symbol=row_limit,
                    extra_columns=extra_columns,
                    extra_column_prefixes=extra_column_prefixes,
                )
                return self._snapshot_from_frame(
                    frame,
                    root=root,
                    exchange=exchange,
                    universe=universe,
                    timeframe=timeframe,
                    source_paths={str(symbol): str(materialized_path) for symbol in sorted(frame["symbol"].astype(str).unique())},
                    materialized_path=str(materialized_path),
                    active_from_column=True,
                )
            membership = _volatile_membership(
                exchange=exchange,
                membership_path=str(membership_path),
                start_ts=start_ts,
                end_ts=end_ts,
                symbols_subset=symbols_subset,
                max_symbols=max_symbols,
            )
            symbols = membership["symbol"].astype(str).drop_duplicates().tolist()
        else:
            symbols = _symbols_from_config(data_cfg.get("symbols"))
            symbols = _apply_symbol_scope(symbols, max_symbols=max_symbols)
            if not symbols:
                raise DataError("MarketDataSnapshot requires stable/volatile universe or explicit data.symbols")

        frames: list[pd.DataFrame] = []
        source_paths: dict[str, str] = {}
        for symbol in symbols:
            path = research_panel_path(root, exchange, symbol, timeframe)
            if not path.exists():
                raise DataError(f"Missing research panel for MarketDataSnapshot symbol={symbol}: {path}")
            frame = _read_panel_frame(
                path,
                start_ts=start_ts,
                end_ts=end_ts,
                row_limit_per_symbol=row_limit,
                extra_columns=extra_columns,
                extra_column_prefixes=extra_column_prefixes,
            )
            if frame.empty:
                continue
            frames.append(frame)
            source_paths[str(symbol)] = str(path)
        if not frames:
            raise DataError("MarketDataSnapshot loaded zero rows")
        combined = pd.concat(frames, ignore_index=True)
        if membership is not None:
            combined = _attach_active_mask_from_membership(combined, membership)
        else:
            combined["volatile_active"] = True
        return self._snapshot_from_frame(
            combined,
            root=root,
            exchange=exchange,
            universe=universe,
            timeframe=timeframe,
            source_paths=source_paths,
            materialized_path=str(materialized_path) if materialized_path else None,
            active_from_column=True,
        )

    def _snapshot_from_single_parquet(self, path: Path) -> MarketDataSnapshot:
        frame = _read_panel_frame(path, start_ts=None, end_ts=None, row_limit_per_symbol=None)
        return self._snapshot_from_frame(
            frame,
            root=path.parent,
            exchange=str(self._data_config().get("exchange", "")),
            universe=str(self._data_config().get("universe", "file")),
            timeframe=str(self._data_config().get("timeframe", "")),
            source_paths={str(symbol): str(path) for symbol in sorted(frame["symbol"].astype(str).unique())},
            materialized_path=str(path),
            active_from_column=False,
        )

    def _snapshot_from_frame(
        self,
        frame: pd.DataFrame,
        *,
        root: Path,
        exchange: str,
        universe: str,
        timeframe: str,
        source_paths: Mapping[str, str],
        materialized_path: str | None,
        active_from_column: bool,
    ) -> MarketDataSnapshot:
        required = set(BAR_COLUMNS)
        missing = required - set(frame.columns)
        if missing:
            raise DataError(f"MarketDataSnapshot missing required columns: {sorted(missing)}")
        work = frame.copy()
        work["ts"] = pd.to_datetime(work["ts"], utc=True)
        work["symbol"] = work["symbol"].astype(str)
        work = work.sort_values(["symbol", "ts"], kind="mergesort").drop_duplicates(["symbol", "ts"], keep="last")
        symbols = tuple(sorted(work["symbol"].drop_duplicates()))
        symbol_to_id = {symbol: idx for idx, symbol in enumerate(symbols)}
        candidate_cols = tuple(candidate_columns(work.columns))
        feature_cols = tuple(col for col in work.columns if col not in set(BAR_COLUMNS) | {"volatile_active", "universe_active"})
        feature_specs = infer_registered_specs_for_columns(feature_cols)
        arrays_by_symbol: dict[str, SymbolArrays] = {}
        feature_arrays_by_symbol: dict[str, dict[str, np.ndarray]] = {}
        for symbol in symbols:
            part = work.loc[work["symbol"].eq(symbol)].reset_index(drop=True)
            extras = {
                col: _column_to_numpy(part[col])
                for col in feature_cols
                if col in part.columns
            }
            active_mask = _active_mask(part, active_from_column=active_from_column)
            candidate_ready = _candidate_ready(part)
            feature_arrays_by_symbol[symbol] = dict(extras)
            arrays_by_symbol[symbol] = SymbolArrays(
                symbol=symbol,
                symbol_id=symbol_to_id[symbol],
                ts=part["ts"].astype("int64").to_numpy(dtype=np.int64, copy=True),
                open=part["open"].to_numpy(dtype=np.float64, copy=True),
                high=part["high"].to_numpy(dtype=np.float64, copy=True),
                low=part["low"].to_numpy(dtype=np.float64, copy=True),
                close=part["close"].to_numpy(dtype=np.float64, copy=True),
                volume=part["volume"].to_numpy(dtype=np.float64, copy=True),
                active_mask=np.ascontiguousarray(active_mask, dtype=bool),
                candidate_ready=np.ascontiguousarray(candidate_ready, dtype=bool),
                extras=extras,
            )
        ts_ns = np.asarray(sorted(work["ts"].astype("int64").drop_duplicates()), dtype=np.int64)
        return MarketDataSnapshot(
            root=root,
            exchange=exchange,
            universe=universe,
            timeframe=timeframe,
            symbols=symbols,
            symbol_to_id=symbol_to_id,
            arrays_by_symbol=arrays_by_symbol,
            ts_ns=np.ascontiguousarray(ts_ns),
            candidate_columns=candidate_cols,
            feature_columns=feature_cols,
            feature_bank=FeatureBank(specs=feature_specs, arrays_by_symbol=feature_arrays_by_symbol),
            source_paths=dict(source_paths),
            materialized_path=materialized_path,
        )


def _read_panel_frame(
    path: Path,
    *,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    row_limit_per_symbol: int | None,
    extra_columns: Iterable[str] | None = None,
    extra_column_prefixes: Iterable[str] | None = None,
) -> pd.DataFrame:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    available = set(parquet_file.schema_arrow.names)
    missing = set(BAR_COLUMNS) - available
    if missing:
        raise DataError(f"MarketDataSnapshot panel missing columns {sorted(missing)} at {path}")
    columns = _read_columns_for_panel(
        available,
        extra_columns=extra_columns,
        extra_column_prefixes=extra_column_prefixes,
    )
    frame = pd.read_parquet(path, columns=columns)
    if frame.empty:
        return frame
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    frame["symbol"] = frame["symbol"].astype(str)
    frame = _filter_time_range(frame, start_ts=start_ts, end_ts=end_ts)
    if row_limit_per_symbol is not None and row_limit_per_symbol > 0:
        frame = frame.groupby("symbol", group_keys=False, sort=False).tail(row_limit_per_symbol)
    return frame.sort_values(["symbol", "ts"], kind="mergesort").reset_index(drop=True)


def _attach_active_mask_from_membership(frame: pd.DataFrame, membership: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    intervals = _membership_intervals(membership)
    out = frame.copy()
    out["volatile_active"] = False
    if intervals.empty:
        return out
    for symbol, symbol_intervals in intervals.groupby("symbol", sort=False):
        mask = out["symbol"].astype(str).eq(str(symbol))
        if not mask.any():
            continue
        symbol_ts = out.loc[mask, "ts"]
        active = np.zeros(len(symbol_ts), dtype=bool)
        for row in symbol_intervals.itertuples(index=False):
            start = pd.Timestamp(row.start_ts)
            end = pd.Timestamp(row.end_ts)
            if pd.isna(end):
                active |= symbol_ts.ge(start).to_numpy()
            else:
                active |= symbol_ts.ge(start).to_numpy() & symbol_ts.lt(end).to_numpy()
        out.loc[mask, "volatile_active"] = active
    return out


def _candidate_ready(frame: pd.DataFrame) -> np.ndarray:
    ready = np.zeros(len(frame), dtype=bool)
    for col in candidate_columns(frame.columns):
        ready |= frame[col].fillna(False).astype(bool).to_numpy()
    if ready.any():
        return ready
    extras = frame.drop(columns=[col for col in BAR_COLUMNS if col in frame.columns], errors="ignore")
    for pos, (_, row) in enumerate(extras.iterrows()):
        if candidate_event_reasons(row.to_dict()):
            ready[pos] = True
    return ready


def _active_mask(frame: pd.DataFrame, *, active_from_column: bool) -> np.ndarray:
    if active_from_column and "volatile_active" in frame.columns:
        return frame["volatile_active"].fillna(False).astype(bool).to_numpy()
    if active_from_column and "universe_active" in frame.columns:
        return frame["universe_active"].fillna(False).astype(bool).to_numpy()
    return np.ones(len(frame), dtype=bool)


def _column_to_numpy(series: pd.Series) -> np.ndarray:
    if pd.api.types.is_bool_dtype(series):
        return np.ascontiguousarray(series.fillna(False).to_numpy(dtype=bool))
    if pd.api.types.is_numeric_dtype(series):
        return np.ascontiguousarray(series.to_numpy(dtype=np.float64, copy=True))
    if pd.api.types.is_datetime64_any_dtype(series):
        return np.ascontiguousarray(pd.to_datetime(series, utc=True).astype("int64").to_numpy(dtype=np.int64, copy=True))
    return np.ascontiguousarray(series.astype(object).to_numpy(copy=True))


def _symbols_from_config(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]  # type: ignore[union-attr]


def _optional_symbols_from_config(value: object) -> list[str] | None:
    symbols = _symbols_from_config(value)
    return symbols or None


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def _apply_symbol_scope(symbols: Iterable[str], *, max_symbols: int | None = None) -> list[str]:
    out = list(dict.fromkeys(str(symbol) for symbol in symbols))
    if max_symbols is not None:
        out = out[:max_symbols]
    return out


__all__ = ["DataSession", "MarketDataSnapshot", "SymbolArrays"]
