"""Fetch-state and coverage manifest management."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts

FETCH_STATE_COLUMNS = (
    "market",
    "exchange",
    "symbol",
    "dataset",
    "timeframe",
    "last_successful_ts",
    "last_attempt_ts",
    "last_row_count",
    "status",
    "error_message",
    "updated_at",
)

COVERAGE_COLUMNS = (
    "market",
    "exchange",
    "symbol",
    "dataset",
    "timeframe",
    "expected_rows",
    "actual_rows",
    "missing_rows",
    "largest_gap_minutes",
    "first_ts",
    "last_ts",
    "updated_at",
)


@dataclass(frozen=True)
class FetchKey:
    market: str
    exchange: str
    symbol: str
    dataset: str
    timeframe: str


class FetchStateStore:
    def __init__(self, store: ResearchDataStore | None = None) -> None:
        self.store = store or ResearchDataStore()
        self.store.ensure_layout()

    @property
    def path(self):
        return self.store.manifest_path("fetch_state")

    def read(self) -> pd.DataFrame:
        df = self.store.read(self.path)
        if df.empty:
            return pd.DataFrame(columns=FETCH_STATE_COLUMNS)
        for col in FETCH_STATE_COLUMNS:
            if col not in df.columns:
                df[col] = "perp" if col == "market" else pd.NA
        for col in ("last_successful_ts", "last_attempt_ts", "updated_at"):
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        return df[list(FETCH_STATE_COLUMNS)]

    def get(self, key: FetchKey) -> pd.Series | None:
        df = self.read()
        if df.empty:
            return None
        mask = (
            df["market"].fillna("perp").astype(str).eq(key.market)
            & df["exchange"].eq(key.exchange)
            & df["symbol"].eq(key.symbol)
            & df["dataset"].eq(key.dataset)
            & df["timeframe"].eq(key.timeframe)
        )
        if not mask.any():
            return None
        return df.loc[mask].iloc[-1]

    def update(
        self,
        key: FetchKey,
        *,
        status: Literal["pending", "running", "success", "failed"],
        last_attempt_ts: object,
        last_successful_ts: object | None = None,
        last_row_count: int = 0,
        error_message: str = "",
    ) -> pd.DataFrame:
        previous = self.get(key)
        if previous is not None:
            if last_successful_ts is None and pd.notna(previous["last_successful_ts"]):
                last_successful_ts = previous["last_successful_ts"]
            if last_row_count == 0 and pd.notna(previous["last_row_count"]):
                last_row_count = int(previous["last_row_count"])
        row = {
            "exchange": key.exchange,
            "market": key.market,
            "symbol": key.symbol,
            "dataset": key.dataset,
            "timeframe": key.timeframe,
            "last_successful_ts": pd.NaT if last_successful_ts is None else utc_ts(last_successful_ts),
            "last_attempt_ts": utc_ts(last_attempt_ts),
            "last_row_count": int(last_row_count),
            "status": status,
            "error_message": error_message,
            "updated_at": utc_ts("now"),
        }
        return self.store.upsert_parquet(
            self.path,
            pd.DataFrame([row], columns=FETCH_STATE_COLUMNS),
            key=("market", "exchange", "symbol", "dataset", "timeframe"),
            columns=FETCH_STATE_COLUMNS,
        )

    def mark_stale_running_failed(
        self,
        *,
        exchange: str,
        market: str = "perp",
        older_than: object,
        error_message: str = "stale running state reset before resume",
    ) -> int:
        """Convert dead running checkpoints to failed without moving progress.

        Fetch checkpoints are written before each chunk starts. If a VM or tmux
        session dies, those rows can remain ``running`` forever. Marking only
        stale rows as failed lets the normal scheduler resume from
        ``last_successful_ts`` or local raw chunks without treating the dead
        worker as active.
        """

        df = self.read()
        if df.empty:
            return 0
        cutoff = utc_ts(older_than)
        updated_at = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
        mask = (
            df["market"].fillna("perp").astype(str).eq(market)
            & df["exchange"].astype(str).eq(exchange)
            & df["status"].astype(str).eq("running")
            & updated_at.notna()
            & updated_at.lt(cutoff)
        )
        count = int(mask.sum())
        if count == 0:
            return 0
        df.loc[mask, "status"] = "failed"
        df.loc[mask, "error_message"] = error_message
        df.loc[mask, "updated_at"] = utc_ts("now")
        self.store.write_atomic(df[list(FETCH_STATE_COLUMNS)], self.path)
        return count


class CoverageStore:
    def __init__(self, store: ResearchDataStore | None = None) -> None:
        self.store = store or ResearchDataStore()
        self.store.ensure_layout()

    @property
    def path(self):
        return self.store.manifest_path("coverage")

    def update_from_dataset(self, exchange: str, symbol: str, dataset: str, timeframe: str, market: str = "perp") -> pd.DataFrame:
        raw_timeframe = "5m" if dataset == "oi" else timeframe
        path = self.store.raw_path(exchange, symbol, dataset, raw_timeframe, market=market)
        df = self.store.read(path)
        row = compute_coverage_row(exchange, symbol, dataset, raw_timeframe, df, market=market)
        return self.store.upsert_parquet(
            self.path,
            pd.DataFrame([row], columns=COVERAGE_COLUMNS),
            key=("market", "exchange", "symbol", "dataset", "timeframe"),
            columns=COVERAGE_COLUMNS,
        )


def expected_frequency(dataset: str, timeframe: str) -> pd.Timedelta | None:
    if dataset in {"ohlcv", "mark", "index"}:
        from bt.research_data.time import timeframe_delta

        return timeframe_delta(timeframe)
    if dataset == "funding":
        return pd.Timedelta(hours=8)
    if dataset == "oi":
        return pd.Timedelta(minutes=5)
    return None


def compute_coverage_row(
    exchange: str,
    symbol: str,
    dataset: str,
    timeframe: str,
    df: pd.DataFrame,
    market: str = "perp",
) -> dict[str, object]:
    freq = expected_frequency(dataset, timeframe)
    if df.empty or "ts" not in df.columns:
        return {
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "dataset": dataset,
            "timeframe": timeframe,
            "expected_rows": 0,
            "actual_rows": 0,
            "missing_rows": 0,
            "largest_gap_minutes": 0.0,
            "first_ts": pd.NaT,
            "last_ts": pd.NaT,
            "updated_at": utc_ts("now"),
        }
    ts = pd.to_datetime(df["ts"], utc=True).sort_values().drop_duplicates()
    first = ts.iloc[0]
    last = ts.iloc[-1]
    actual = len(ts)
    expected = actual
    missing = 0
    largest_gap_minutes = 0.0
    if freq is not None and actual > 1:
        expected = int(((last - first) / freq)) + 1
        missing = max(expected - actual, 0)
        max_gap = ts.diff().dropna().max()
        largest_gap_minutes = float(max_gap / pd.Timedelta(minutes=1)) if pd.notna(max_gap) else 0.0
    return {
        "exchange": exchange,
        "market": market,
        "symbol": symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "expected_rows": int(expected),
        "actual_rows": int(actual),
        "missing_rows": int(missing),
        "largest_gap_minutes": largest_gap_minutes,
        "first_ts": first,
        "last_ts": last,
        "updated_at": utc_ts("now"),
    }
