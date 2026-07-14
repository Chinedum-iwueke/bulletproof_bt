"""Instrument manifest helpers."""
from __future__ import annotations

import pandas as pd

from bt.research_data.config import STABLE_SYMBOL_ALIASES, STABLE_USDT_PERP_SYMBOLS
from bt.research_data.schemas import INSTRUMENT_COLUMNS, normalize_frame
from bt.research_data.storage import ResearchDataStore
from bt.research_data.time import utc_ts


def canonical_perp_symbol(base_asset: str | None, quote_asset: str | None = "USDT") -> str:
    base = (base_asset or "").upper()
    quote = (quote_asset or "USDT").upper()
    return f"{base}-{quote}-PERP"


def canonical_spot_symbol(base_asset: str | None, quote_asset: str | None = "USDT") -> str:
    base = (base_asset or "").upper()
    quote = (quote_asset or "USDT").upper()
    return f"{base}-{quote}-SPOT"


def _precision_from_step(value: object) -> int | None:
    if value is None or value == "" or pd.isna(value):
        return None
    text = str(value).rstrip("0").rstrip(".")
    if "." not in text:
        return 0
    return max(len(text.split(".", 1)[1]), 0)


def normalize_instrument_frame(rows: list[dict[str, object]] | pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(rows).copy()
    if df.empty:
        return pd.DataFrame(columns=INSTRUMENT_COLUMNS)
    if "native_symbol" not in df.columns and "symbol" in df.columns:
        df["native_symbol"] = df["symbol"]
    if "market" not in df.columns:
        df["market"] = "perp"
    if "canonical_symbol" not in df.columns:
        canonical = []
        for market, base, quote in zip(
            df.get("market", pd.Series(dtype=object)),
            df.get("base_asset", pd.Series(dtype=object)),
            df.get("quote_asset", pd.Series(dtype=object)),
        ):
            canonical.append(
                canonical_spot_symbol(base, quote)
                if str(market).lower() == "spot"
                else canonical_perp_symbol(base, quote)
            )
        df["canonical_symbol"] = canonical
    if "first_seen_ts" not in df.columns and "onboard_ts" in df.columns:
        df["first_seen_ts"] = df["onboard_ts"]
    if "settle_asset" not in df.columns and "margin_asset" in df.columns:
        df["settle_asset"] = df["margin_asset"]
    if "price_precision" not in df.columns and "tick_size" in df.columns:
        df["price_precision"] = df["tick_size"].map(_precision_from_step)
    if "qty_precision" not in df.columns and "qty_step" in df.columns:
        df["qty_precision"] = df["qty_step"].map(_precision_from_step)
    df["last_seen_ts"] = df.get("last_seen_ts", utc_ts("now"))
    return normalize_frame(df, INSTRUMENT_COLUMNS)


def canonical_for_native(instruments: pd.DataFrame, exchange: str, native_symbol: str, market: str = "perp") -> str:
    if instruments.empty:
        return native_to_canonical_symbol(native_symbol, market=market)
    native_col = "native_symbol" if "native_symbol" in instruments.columns else "symbol"
    mask = instruments["exchange"].eq(exchange) & instruments[native_col].astype(str).eq(native_symbol)
    if "market" in instruments.columns:
        mask &= instruments["market"].fillna("perp").astype(str).str.lower().eq(str(market).lower())
    if mask.any() and "canonical_symbol" in instruments.columns:
        return str(instruments.loc[mask, "canonical_symbol"].iloc[0])
    return native_to_canonical_symbol(native_symbol, market=market)


def native_to_canonical_symbol(native_symbol: str, market: str = "perp") -> str:
    symbol = native_symbol.upper()
    if str(market).lower() == "spot":
        if symbol.endswith("USDT"):
            return canonical_spot_symbol(symbol.removesuffix("USDT"), "USDT")
        return f"{symbol}-SPOT"
    if symbol.endswith("-SWAP"):
        parts = symbol.split("-")
        if len(parts) >= 2:
            return canonical_perp_symbol(parts[0], parts[1])
    if symbol.endswith("USDT"):
        return canonical_perp_symbol(symbol.removesuffix("USDT"), "USDT")
    return symbol


def reconcile_stable_symbols(instruments: pd.DataFrame, exchange: str | None = None, market: str = "perp") -> pd.DataFrame:
    """Map configured stable symbols onto exchange listings without failing on late listings."""
    rows: list[dict[str, object]] = []
    native_col = "native_symbol" if "native_symbol" in instruments.columns else "symbol"
    exchange_name = exchange or (
        str(instruments["exchange"].dropna().iloc[0]) if "exchange" in instruments.columns and not instruments["exchange"].dropna().empty else "binance"
    )
    available = set(instruments.get(native_col, pd.Series(dtype=str)).astype(str))
    indexed = instruments.set_index(native_col, drop=False) if native_col in instruments.columns else pd.DataFrame()
    canonical_indexed = (
        instruments.set_index("canonical_symbol", drop=False)
        if "canonical_symbol" in instruments.columns
        else pd.DataFrame()
    )
    for configured in STABLE_USDT_PERP_SYMBOLS:
        candidates = STABLE_SYMBOL_ALIASES.get(configured, (configured,))
        selected = next((candidate for candidate in candidates if candidate in available), None)
        if selected is None and not canonical_indexed.empty:
            canonical_candidates = [native_to_canonical_symbol(candidate, market=market) for candidate in candidates]
            selected_canonical = next((candidate for candidate in canonical_candidates if candidate in canonical_indexed.index), None)
            if selected_canonical is not None:
                matched = canonical_indexed.loc[selected_canonical, native_col]
                selected = str(matched.iloc[0] if hasattr(matched, "iloc") else matched)
        row = {
            "configured_symbol": configured,
            "market": market,
            "symbol": selected or candidates[0],
            "native_symbol": selected or candidates[0],
            "exchange": exchange_name,
            "universe": "stable_data_1m_canonical",
            "available": selected is not None,
            "first_seen_ts": pd.NaT,
        }
        if selected is not None:
            if "first_seen_ts" in indexed.columns:
                row["first_seen_ts"] = indexed.loc[selected, "first_seen_ts"]
            elif "onboard_ts" in indexed.columns:
                row["first_seen_ts"] = indexed.loc[selected, "onboard_ts"]
        rows.append(row)
    return pd.DataFrame(rows)


def write_instrument_manifest(store: ResearchDataStore, instruments: pd.DataFrame) -> None:
    store.ensure_layout()
    normalized = normalize_instrument_frame(instruments)
    store.upsert_parquet(
        store.manifest_path("instruments"),
        normalized,
        key=("market", "exchange", "native_symbol"),
        columns=INSTRUMENT_COLUMNS,
    )


def write_stable_universe(
    store: ResearchDataStore,
    instruments: pd.DataFrame,
    exchange: str | None = None,
    market: str = "perp",
) -> pd.DataFrame:
    stable = reconcile_stable_symbols(instruments, exchange, market=market)
    stable["created_ts"] = utc_ts("now")
    manifest_name = "stable_universe" if market == "perp" else f"stable_universe_{market}"
    store.upsert_parquet(
        store.manifest_path(manifest_name),
        stable,
        key=("market", "exchange", "configured_symbol"),
    )
    return stable
