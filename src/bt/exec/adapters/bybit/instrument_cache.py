from __future__ import annotations

from dataclasses import dataclass

from bt.exec.adapters.bybit.client_rest import BybitRESTClient


@dataclass(frozen=True)
class InstrumentSpec:
    symbol: str
    tick_size: float
    lot_size: float


class BybitInstrumentCache:
    def __init__(self, *, rest_client: BybitRESTClient, category: str) -> None:
        self._rest_client = rest_client
        self._category = category
        self._cache: dict[str, InstrumentSpec] = {}

    def get(self, symbol: str) -> InstrumentSpec | None:
        if symbol in self._cache:
            return self._cache[symbol]
        response = self._rest_client.get_private("/v5/market/instruments-info", params={"category": self._category, "symbol": symbol})
        items = response.result.get("list") if isinstance(response.result.get("list"), list) else []
        if not items:
            return None
        first = items[0]
        if not isinstance(first, dict):
            return None
        lot_filter = first.get("lotSizeFilter") if isinstance(first.get("lotSizeFilter"), dict) else {}
        price_filter = first.get("priceFilter") if isinstance(first.get("priceFilter"), dict) else {}
        spec = InstrumentSpec(
            symbol=str(first.get("symbol", symbol)),
            tick_size=float(price_filter.get("tickSize", 0.0) or 0.0),
            lot_size=float(lot_filter.get("qtyStep", 0.0) or 0.0),
        )
        self._cache[symbol] = spec
        return spec
