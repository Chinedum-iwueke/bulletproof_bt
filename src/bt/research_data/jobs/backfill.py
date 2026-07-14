"""Backfill raw Binance datasets."""
from __future__ import annotations

import pandas as pd

from bt.research_data.config import RAW_DATASETS, SPOT_RAW_DATASETS
from bt.research_data.fetching.state import FETCH_STATE_COLUMNS
from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.instruments import write_instrument_manifest, write_stable_universe
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts


def backfill(
    exchange: str,
    symbols: list[str],
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    datasets: list[str] | None = None,
    timeframe: str = "1m",
    store: ResearchDataStore | None = None,
    market: str = "perp",
) -> None:
    adapter = get_adapter(exchange, market=market)
    store = store or ResearchDataStore()
    store.ensure_layout()
    start_ts = utc_ts(start)
    end_ts = utc_ts(end)
    selected = datasets or (list(SPOT_RAW_DATASETS) if market == "spot" else list(RAW_DATASETS))
    fetchers = {
        "ohlcv": lambda symbol: (
            adapter.fetch_spot_ohlcv(symbol, start_ts, end_ts, timeframe)
            if market == "spot"
            else adapter.fetch_ohlcv(symbol, start_ts, end_ts, timeframe)
        ),
        "mark": lambda symbol: adapter.fetch_mark(symbol, start_ts, end_ts, timeframe),
        "index": lambda symbol: adapter.fetch_index(symbol, start_ts, end_ts, timeframe),
        "funding": lambda symbol: adapter.fetch_funding(symbol, start_ts, end_ts),
        "oi": lambda symbol: adapter.fetch_open_interest(symbol, start_ts, end_ts),
    }
    if market == "spot":
        unsupported = sorted(set(selected) - set(SPOT_RAW_DATASETS))
        if unsupported:
            raise ValueError(f"spot market supports only datasets {SPOT_RAW_DATASETS}; got {unsupported}")
    fetch_state_rows: list[dict[str, object]] = []
    failures: list[str] = []
    for symbol in symbols:
        for dataset in selected:
            status = "ok"
            error = ""
            try:
                df = fetchers[dataset](symbol)
                store.upsert_dataset(exchange, symbol, dataset, timeframe if dataset != "oi" else "5m", df, market=market)
            except Exception as exc:
                status = "failed"
                error = str(exc)
                df = pd.DataFrame()
            fetch_state_rows.append(
                {
                    "market": market,
                    "exchange": exchange,
                    "symbol": symbol,
                    "dataset": dataset,
                    "timeframe": timeframe if dataset != "oi" else "5m",
                    "last_successful_ts": end_ts if status == "ok" else pd.NaT,
                    "last_attempt_ts": end_ts,
                    "last_row_count": len(df),
                    "status": status,
                    "error_message": error,
                    "updated_at": utc_ts("now"),
                }
            )
            if status == "failed":
                failures.append(f"{exchange} {symbol} {dataset}: {error}")
    if fetch_state_rows:
        store.upsert_parquet(
            store.manifest_path("fetch_state"),
            pd.DataFrame(fetch_state_rows, columns=FETCH_STATE_COLUMNS),
            key=("market", "exchange", "symbol", "dataset", "timeframe"),
            columns=FETCH_STATE_COLUMNS,
        )
    if failures:
        raise RuntimeError("backfill failures: " + "; ".join(failures))


def backfill_stable(
    exchange: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    timeframe: str = "1m",
    store: ResearchDataStore | None = None,
    market: str = "perp",
) -> None:
    adapter = get_adapter(exchange, market=market)
    store = store or ResearchDataStore()
    instruments = adapter.fetch_spot_instruments() if market == "spot" else adapter.fetch_usdt_perp_instruments()
    write_instrument_manifest(store, instruments)
    stable = write_stable_universe(store, instruments, exchange=exchange, market=market)
    symbols = stable.loc[stable["available"], "native_symbol"].astype(str).tolist()
    datasets = list(SPOT_RAW_DATASETS) if market == "spot" else list(RAW_DATASETS)
    backfill(exchange, symbols, start, end, datasets, timeframe, store, market=market)
