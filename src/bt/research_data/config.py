"""Configuration for the research data subsystem."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

RESEARCH_DATA_ROOT = Path("research_data")
DEFAULT_EXCHANGE = "binance"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_START_TS = pd.Timestamp("2021-01-01T00:00:00Z")

RAW_DATASETS = ("ohlcv", "mark", "index", "funding", "oi")
SPOT_RAW_DATASETS = ("ohlcv",)

# Stable major/stable Binance USDT perpetual research basket. Some symbols were
# listed after 2021 or migrated over time; instrument reconciliation skips absent
# symbols and records availability instead of manufacturing a backtest universe.
STABLE_USDT_PERP_SYMBOLS: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "TRXUSDT",
    "DOTUSDT",
    "LTCUSDT",
    "BCHUSDT",
    "NEARUSDT",
    "ATOMUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "FILUSDT",
    "ETCUSDT",
    "INJUSDT",
    "SUIUSDT",
    "AAVEUSDT",
    "UNIUSDT",
    "MKRUSDT",
    "RNDRUSDT",
    "SEIUSDT",
    "POLUSDT",
    "FETUSDT",
    "TONUSDT",
)

STABLE_SYMBOL_ALIASES: dict[str, tuple[str, ...]] = {
    "POLUSDT": ("POLUSDT", "MATICUSDT"),
    "FETUSDT": ("FETUSDT", "ASIUSDT"),
}

BINANCE_FAPI_BASE_URL = "https://fapi.binance.com"
BYBIT_V5_BASE_URL = "https://api.bybit.com"
OKX_V5_BASE_URL = "https://www.okx.com"
BINANCE_SPOT_BASE_URL = "https://api.binance.com"
