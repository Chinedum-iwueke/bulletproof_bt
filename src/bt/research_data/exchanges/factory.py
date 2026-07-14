"""Exchange adapter factory."""
from __future__ import annotations

from bt.research_data.exchanges.binance import BinanceSpotAdapter, BinanceUSDMPerpAdapter
from bt.research_data.exchanges.bybit import BybitSpotAdapter, BybitUSDTPerpAdapter
from bt.research_data.exchanges.okx import OKXUSDTPerpAdapter


def get_adapter(exchange: str, market: str = "perp"):
    name = exchange.lower()
    market_name = str(market or "perp").lower()
    if name == "binance":
        if market_name == "spot":
            return BinanceSpotAdapter()
        return BinanceUSDMPerpAdapter()
    if name == "bybit":
        if market_name == "spot":
            return BybitSpotAdapter()
        return BybitUSDTPerpAdapter()
    if name == "okx":
        return OKXUSDTPerpAdapter()
    raise ValueError(f"unsupported exchange: {exchange}")
