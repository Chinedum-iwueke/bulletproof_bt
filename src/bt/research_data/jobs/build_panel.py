"""Build canonical symbol panels."""
from __future__ import annotations

import pandas as pd

from bt.research_data.alignment import build_research_panel
from bt.research_data.schemas import SCHEMAS
from bt.research_data.storage import ResearchDataStore


def _build_spot_panel(ohlcv: pd.DataFrame) -> pd.DataFrame:
    panel = ohlcv.copy()
    for col in SCHEMAS["research_panel"].columns:
        if col not in panel.columns:
            panel[col] = pd.NA
    return panel[list(SCHEMAS["research_panel"].columns)]


def build_panels(
    exchange: str,
    symbols: list[str],
    timeframe: str = "1m",
    store: ResearchDataStore | None = None,
    market: str = "perp",
) -> None:
    store = store or ResearchDataStore()
    for symbol in symbols:
        ohlcv = store.read(store.raw_path(exchange, symbol, "ohlcv", timeframe, market=market))
        if market == "spot":
            panel = _build_spot_panel(ohlcv)
        else:
            mark = store.read(store.raw_path(exchange, symbol, "mark", timeframe, market=market))
            index = store.read(store.raw_path(exchange, symbol, "index", timeframe, market=market))
            funding = store.read(store.raw_path(exchange, symbol, "funding", timeframe, market=market))
            oi = store.read(store.raw_path(exchange, symbol, "oi", "5m", market=market))
            liquidations = store.read(store.canonical_path(exchange, symbol, timeframe, "liquidation_1m", market=market))
            panel = build_research_panel(ohlcv, mark, index, funding, oi, liquidations)
        store.upsert_parquet(
            store.canonical_path(exchange, symbol, timeframe, "research_panel", market=market),
            panel,
            key=SCHEMAS["research_panel"].key,
        )
        if not ohlcv.empty:
            store.upsert_parquet(
                store.canonical_path(exchange, symbol, timeframe, "ohlcv", market=market),
                ohlcv,
                key=SCHEMAS["ohlcv"].key,
                columns=SCHEMAS["ohlcv"].columns,
            )
        if market == "spot":
            continue
        perp_cols = [
            col
            for col in panel.columns
            if col
            in {
                "ts",
                "exchange",
                "symbol",
                "funding_rate",
                "funding_source_ts",
                "open_interest",
                "oi_source_ts",
                "oi_change_1",
                "oi_change_pct_1",
                "premium_mark_vs_index",
                "basis_close_vs_index",
            }
        ]
        if not panel.empty:
            store.upsert_parquet(
                store.canonical_path(exchange, symbol, timeframe, "perp_features", market=market),
                panel[perp_cols],
                key=("exchange", "symbol", "ts"),
            )
