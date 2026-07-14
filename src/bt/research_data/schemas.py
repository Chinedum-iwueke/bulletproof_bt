"""Canonical schemas and normalization helpers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from bt.research_data.time import utc_series

OHLCV_COLUMNS = (
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
    "trade_count",
)
MARK_COLUMNS = (
    "ts",
    "exchange",
    "symbol",
    "canonical_symbol",
    "mark_open",
    "mark_high",
    "mark_low",
    "mark_close",
)
INDEX_COLUMNS = (
    "ts",
    "exchange",
    "symbol",
    "canonical_symbol",
    "index_open",
    "index_high",
    "index_low",
    "index_close",
)
FUNDING_COLUMNS = ("ts", "exchange", "symbol", "canonical_symbol", "funding_rate", "mark_price_at_funding")
OI_COLUMNS = ("ts", "exchange", "symbol", "canonical_symbol", "open_interest", "open_interest_value")
LIQUIDATION_EVENT_COLUMNS = (
    "ts",
    "exchange",
    "native_symbol",
    "canonical_symbol",
    "side",
    "price",
    "qty",
    "notional",
    "event_id",
    "raw",
)
LIQUIDATION_1M_COLUMNS = (
    "ts",
    "exchange",
    "native_symbol",
    "canonical_symbol",
    "liq_buy_qty",
    "liq_sell_qty",
    "liq_buy_notional",
    "liq_sell_notional",
    "liq_event_count",
)
VOLATILE_UNIVERSE_COLUMNS = (
    "ts",
    "exchange",
    "symbol",
    "canonical_symbol",
    "universe",
    "rank_type",
    "rank",
    "score",
    "rebalance_id",
    "lookback",
    "rebalance_freq",
)

INSTRUMENT_COLUMNS = (
    "market",
    "exchange",
    "native_symbol",
    "canonical_symbol",
    "base_asset",
    "quote_asset",
    "settle_asset",
    "contract_type",
    "status",
    "first_seen_ts",
    "last_seen_ts",
    "price_precision",
    "qty_precision",
)
RESEARCH_PANEL_COLUMNS = (
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
    "mark_open",
    "mark_high",
    "mark_low",
    "mark_close",
    "index_open",
    "index_high",
    "index_low",
    "index_close",
    "funding_rate",
    "funding_source_ts",
    "open_interest",
    "oi_source_ts",
    "oi_change_1",
    "oi_change_pct_1",
    "premium_mark_vs_index",
    "basis_close_vs_index",
)


@dataclass(frozen=True)
class DatasetSchema:
    columns: tuple[str, ...]
    key: tuple[str, ...]


SCHEMAS: dict[str, DatasetSchema] = {
    "ohlcv": DatasetSchema(OHLCV_COLUMNS, ("exchange", "symbol", "ts")),
    "mark": DatasetSchema(MARK_COLUMNS, ("exchange", "symbol", "ts")),
    "index": DatasetSchema(INDEX_COLUMNS, ("exchange", "symbol", "ts")),
    "funding": DatasetSchema(FUNDING_COLUMNS, ("exchange", "symbol", "ts")),
    "oi": DatasetSchema(OI_COLUMNS, ("exchange", "symbol", "ts")),
    "volatile_universe": DatasetSchema(VOLATILE_UNIVERSE_COLUMNS, ("exchange", "ts", "symbol", "universe")),
    "research_panel": DatasetSchema(RESEARCH_PANEL_COLUMNS, ("exchange", "symbol", "ts")),
}


def normalize_frame(df: pd.DataFrame, columns: Iterable[str] | None = None) -> pd.DataFrame:
    """Normalize UTC timestamps and column order without interpolating values."""
    out = df.copy()
    if "ts" in out.columns:
        out["ts"] = utc_series(out["ts"])
    for source_col in ("funding_source_ts", "oi_source_ts", "first_seen_ts", "last_seen_ts", "delivery_ts", "onboard_ts"):
        if source_col in out.columns:
            out[source_col] = pd.to_datetime(out[source_col], utc=True, errors="coerce")
    if columns is not None:
        for col in columns:
            if col not in out.columns:
                out[col] = "perp" if col == "market" else pd.NA
            elif col == "market":
                out[col] = out[col].fillna("perp")
        out = out[list(columns)]
    return out
