"""Streaming per-symbol bar source with strict row validation."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from bt.data.market_rules import MarketRules, parse_market_rules, validate_market_timestamp
from bt.data.parquet_io import ensure_pyarrow_parquet


RowTuple = tuple[pd.Timestamp, float, float, float, float, float]


@dataclass(frozen=True)
class DateRange:
    start: pd.Timestamp | None
    end: pd.Timestamp | None


def _parse_ts_utc(value: Any, *, symbol: str, row_number: int) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        raise ValueError(f"{symbol}: row {row_number} ts must be timezone-aware UTC")
    ts = ts.tz_convert("UTC")
    return ts


def _to_float(value: Any, *, symbol: str, row_number: int, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{symbol}: row {row_number} field '{field}' must be numeric") from exc


class SymbolDataSource:
    """Iterate symbol bars from a CSV/parquet file one row at a time."""

    def __init__(
        self,
        symbol: str,
        path: str,
        *,
        date_range: dict[str, Any] | tuple[Any, Any] | None = None,
        row_limit: int | None = None,
        chunksize: int = 50_000,
        market_rules: MarketRules | None = None,
    ) -> None:
        if not symbol:
            raise ValueError("symbol must be a non-empty string")
        self._symbol = symbol
        self._path = Path(path)
        if not self._path.is_file():
            raise ValueError(f"{symbol}: data file not found: {path}")
        if row_limit is not None and row_limit <= 0:
            raise ValueError("row_limit must be positive when provided")
        if chunksize <= 0:
            raise ValueError("chunksize must be positive")

        self._row_limit = row_limit
        self._chunksize = chunksize
        self._date_range = self._parse_date_range(date_range)
        self._market_rules = market_rules or parse_market_rules({"data": {"market": "crypto_24x7"}})

    def _parse_date_range(self, date_range: dict[str, Any] | tuple[Any, Any] | None) -> DateRange:
        if date_range is None:
            return DateRange(start=None, end=None)

        if isinstance(date_range, dict):
            start_raw = date_range.get("start")
            end_raw = date_range.get("end")
        elif isinstance(date_range, tuple) and len(date_range) == 2:
            start_raw, end_raw = date_range
        else:
            raise ValueError("date_range must be a (start, end) tuple or mapping with optional start/end")
        start = pd.Timestamp(start_raw).tz_convert("UTC") if start_raw is not None else None
        end = pd.Timestamp(end_raw).tz_convert("UTC") if end_raw is not None else None
        if start is not None and end is not None and start > end:
            raise ValueError("date_range start must be <= end")
        return DateRange(start=start, end=end)

    def __iter__(self) -> Iterator[RowTuple]:
        suffix = self._path.suffix.lower()
        if suffix == ".csv":
            yield from self._iter_csv()
            return
        if suffix == ".parquet":
            yield from self._iter_parquet()
            return
        raise ValueError(f"{self._symbol}: unsupported file extension: {self._path.suffix}")

    def _iter_csv(self) -> Iterator[RowTuple]:
        emitted = 0
        last_ts: pd.Timestamp | None = None
        for chunk in pd.read_csv(self._path, chunksize=self._chunksize):
            for row in chunk.itertuples(index=False):
                as_dict = row._asdict()
                validated = self._validate_row(as_dict, emitted + 1, last_ts)
                if validated is None:
                    continue
                yield validated
                emitted += 1
                last_ts = validated[0]
                if self._row_limit is not None and emitted >= self._row_limit:
                    return

    def _iter_parquet(self) -> Iterator[RowTuple]:
        emitted = 0
        last_ts: pd.Timestamp | None = None

        ensure_pyarrow_parquet()

        try:
            import pyarrow as pa
        except ImportError:
            frame = pd.read_parquet(self._path)
            batches = [frame]
        else:
            source = pa.memory_map(str(self._path), "r")
            parquet_file = pa.parquet.ParquetFile(source)
            batches = (
                batch.to_pandas()
                for batch in parquet_file.iter_batches(batch_size=self._chunksize)
            )

        for batch_df in batches:
            for row in batch_df.itertuples(index=False):
                as_dict = row._asdict()
                validated = self._validate_row(as_dict, emitted + 1, last_ts)
                if validated is None:
                    continue
                yield validated
                emitted += 1
                last_ts = validated[0]
                if self._row_limit is not None and emitted >= self._row_limit:
                    return

    def _validate_row(
        self,
        row: dict[str, Any],
        row_number: int,
        last_ts: pd.Timestamp | None,
    ) -> RowTuple | None:
        normalized = {str(key).strip().lower(): value for key, value in row.items()}
        required = ["ts", "open", "high", "low", "close", "volume"]
        missing = [col for col in required if col not in normalized]
        if missing:
            raise ValueError(f"{self._symbol}: missing required column(s): {missing}")

        row_symbol = normalized.get("symbol")
        if row_symbol is not None and str(row_symbol) != self._symbol:
            raise ValueError(
                f"{self._symbol}: encountered mismatched symbol value '{row_symbol}' in file"
            )

        ts = _parse_ts_utc(normalized["ts"], symbol=self._symbol, row_number=row_number)
        if last_ts is not None and ts <= last_ts:
            raise ValueError(
                f"{self._symbol}: non-monotonic ts in {self._path}; row {row_number} has {ts}"
            )

        validate_market_timestamp(
            market_rules=self._market_rules,
            symbol=self._symbol,
            ts_utc=ts.to_pydatetime(),
            path=str(self._path),
        )

        start, end = self._date_range.start, self._date_range.end
        if start is not None and ts < start:
            return None
        if end is not None and ts >= end:
            return None

        o = _to_float(normalized["open"], symbol=self._symbol, row_number=row_number, field="open")
        h = _to_float(normalized["high"], symbol=self._symbol, row_number=row_number, field="high")
        l = _to_float(normalized["low"], symbol=self._symbol, row_number=row_number, field="low")
        c = _to_float(normalized["close"], symbol=self._symbol, row_number=row_number, field="close")
        v = _to_float(normalized["volume"], symbol=self._symbol, row_number=row_number, field="volume")

        if l > min(o, c) or h < max(o, c) or h < l:
            raise ValueError(
                f"{self._symbol}: invalid OHLC at row {row_number} in {self._path}: "
                f"open={o}, high={h}, low={l}, close={c}"
            )
        if v < 0:
            raise ValueError(f"{self._symbol}: negative volume at row {row_number} in {self._path}: {v}")

        return (ts, o, h, l, c, v)
