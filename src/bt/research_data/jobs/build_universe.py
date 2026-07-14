"""Build universe manifests."""
from __future__ import annotations

import pandas as pd

from bt.research_data.exchanges.factory import get_adapter
from bt.research_data.instruments import write_instrument_manifest
from bt.research_data.schemas import VOLATILE_UNIVERSE_COLUMNS, normalize_frame
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts
from bt.research_data.universe import rank_volatile_scores, score_volatile_symbol_from_ohlcv


def _chunk_start_ts(path) -> pd.Timestamp | None:
    try:
        stamp = path.name.split("-")[1]
        return pd.Timestamp(stamp, tz="UTC")
    except Exception:
        return None


def _read_ohlcv_for_universe(path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    columns = ["ts", "exchange", "symbol", "close", "volume", "quote_volume"]
    frames: list[pd.DataFrame] = []
    if path.exists():
        frames.append(pd.read_parquet(path, columns=columns))
    chunk_root = path.parent / "chunks"
    if chunk_root.exists():
        for chunk_path in sorted(chunk_root.glob("year=*/month=*/*.parquet")):
            chunk_start = _chunk_start_ts(chunk_path)
            if chunk_start is not None and (chunk_start < start - pd.Timedelta(days=8) or chunk_start > end):
                continue
            frames.append(pd.read_parquet(chunk_path, columns=columns))
    if not frames:
        return pd.DataFrame(columns=columns)
    out = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    if out.empty:
        return pd.DataFrame(columns=columns)
    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out = out[(out["ts"] >= start - pd.Timedelta(days=8)) & (out["ts"] <= end)]
    return out.drop_duplicates(["exchange", "symbol", "ts"], keep="last").sort_values(["symbol", "ts"]).reset_index(drop=True)


def _candidate_raw_paths(
    store: ResearchDataStore,
    exchange: str,
    symbol: str,
    dataset: str,
    timeframe: str,
    market: str,
) -> list:
    paths = [store.raw_path(exchange, symbol, dataset, timeframe, market=market)]
    if market == "perp":
        paths.append(store.legacy_raw_path(exchange, symbol, dataset, timeframe))
    return paths


def build_volatile_universe(
    exchange: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    rebalance_freq: str,
    lookback: str,
    top_gainers: int,
    top_losers: int,
    min_age_days: int,
    min_median_dollar_volume_7d: float,
    store: ResearchDataStore | None = None,
    market: str = "perp",
) -> pd.DataFrame:
    store = store or ResearchDataStore()
    adapter = get_adapter(exchange, market=market)
    try:
        instruments = adapter.fetch_spot_instruments() if market == "spot" else adapter.fetch_usdt_perp_instruments()
        write_instrument_manifest(store, instruments)
    except Exception:
        manifest_path = store.manifest_path("instruments")
        if not manifest_path.exists():
            raise
        instruments = store.read(manifest_path)
        if "market" in instruments.columns:
            instruments = instruments[instruments["market"].fillna("perp").astype(str).eq(market)]
        if "exchange" in instruments.columns:
            instruments = instruments[instruments["exchange"].eq(exchange)]
        if instruments.empty:
            raise
    start_ts = utc_ts(start)
    end_ts = utc_ts(end)
    stable_symbols: set[str] = set()
    stable_path = store.manifest_path("stable_universe" if market == "perp" else f"stable_universe_{market}")
    if stable_path.exists():
        stable = store.read(stable_path)
        if not stable.empty and "market" in stable.columns:
            stable = stable[stable["market"].fillna("perp").astype(str).eq(market)]
        if not stable.empty and "exchange" in stable.columns:
            stable = stable[stable["exchange"].eq(exchange)]
        if "available" in stable.columns:
            stable = stable[stable["available"].astype(bool)]
        native = "native_symbol" if "native_symbol" in stable.columns else "symbol"
        if native in stable.columns:
            stable_symbols = set(stable[native].astype(str))
    score_frames = []
    native_col = "native_symbol" if "native_symbol" in instruments.columns else "symbol"
    symbols = [symbol for symbol in instruments[native_col].astype(str).tolist() if symbol not in stable_symbols]
    for idx, symbol in enumerate(symbols, start=1):
        paths = _candidate_raw_paths(store, exchange, symbol, "ohlcv", "1m", market)
        existing_paths = [path for path in paths if path.exists() or (path.parent / "chunks").exists()]
        if existing_paths:
            path = existing_paths[0]
            frame = _read_ohlcv_for_universe(path, start_ts, end_ts)
            if not frame.empty:
                instrument_row = instruments.loc[instruments[native_col].astype(str).eq(symbol)]
                first_seen_ts = None
                if not instrument_row.empty and "first_seen_ts" in instrument_row.columns:
                    first_seen_ts = instrument_row["first_seen_ts"].iloc[0]
                scores = score_volatile_symbol_from_ohlcv(
                    frame,
                    start=start_ts,
                    end=end_ts,
                    first_seen_ts=first_seen_ts,
                    rebalance_freq=rebalance_freq,
                    lookback=lookback,
                    min_age_days=min_age_days,
                    min_median_dollar_volume_7d=min_median_dollar_volume_7d,
                )
                if not scores.empty:
                    score_frames.append(scores)
        if idx == 1 or idx % 50 == 0 or idx == len(symbols):
            print(f"{exchange} volatile universe loaded {idx}/{len(symbols)} instruments", flush=True)
    scores = pd.concat(score_frames, ignore_index=True) if score_frames else pd.DataFrame()
    print(
        f"{exchange} volatile universe ranking scores={len(scores)} symbols={scores['symbol'].nunique() if not scores.empty else 0}",
        flush=True,
    )
    membership = rank_volatile_scores(
        scores,
        exchange=exchange,
        rebalance_freq=rebalance_freq,
        lookback=lookback,
        top_gainers=top_gainers,
        top_losers=top_losers,
    )
    if not membership.empty:
        membership["market"] = market
    print(
        f"{exchange} volatile universe membership rows={len(membership)} symbols={membership['symbol'].nunique() if not membership.empty else 0}",
        flush=True,
    )
    membership_path = store.manifest_path(
        "volatile_universe_membership" if market == "perp" else f"volatile_universe_membership_{market}"
    )
    with store.write_lock():
        existing = store.read(membership_path)
        if not existing.empty and "market" in existing.columns:
            existing = existing[existing["market"].fillna("perp").astype(str).eq(market)]
        if not existing.empty and "exchange" in existing.columns:
            existing = existing[~existing["exchange"].eq(exchange)]
        frames = [frame for frame in (existing, membership) if not frame.empty]
        combined = pd.concat(frames, ignore_index=True) if frames else membership
        columns = tuple(VOLATILE_UNIVERSE_COLUMNS) + (("market",) if "market" in combined.columns else ())
        combined = normalize_frame(combined, columns)
        if not combined.empty:
            combined = combined.drop_duplicates(
                subset=["market", "exchange", "ts", "symbol", "universe", "rank_type"] if "market" in combined.columns else ["exchange", "ts", "symbol", "universe", "rank_type"],
                keep="last",
            ).sort_values(["exchange", "ts", "symbol", "rank_type"])
        store.write_atomic(combined.reset_index(drop=True), membership_path)
    return membership
