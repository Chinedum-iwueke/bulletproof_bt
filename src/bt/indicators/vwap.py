"""Volume-weighted average price indicators."""
from __future__ import annotations

import pandas as pd

from bt.core.types import Bar
from bt.indicators.base import BaseIndicator, safe_div
from bt.indicators.registry import register


@register("vwap")
class VWAP(BaseIndicator):
    """Streaming cumulative VWAP using typical price.

    Formula:
        VWAP_t = sum(((high+low+close)/3) * volume) / sum(volume)
    """

    def __init__(self) -> None:
        super().__init__(name="vwap", warmup_bars=1)
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        typical_price = (bar.high + bar.low + bar.close) / 3
        self._cum_pv += typical_price * bar.volume
        self._cum_vol += bar.volume

    def reset(self) -> None:
        self._bars_seen = 0
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    @property
    def is_ready(self) -> bool:
        return self._cum_vol > 0

    @property
    def value(self) -> float | None:
        if self._cum_vol <= 0:
            return None
        return safe_div(self._cum_pv, self._cum_vol, default=0.0)


@register("session_vwap")
class SessionVWAP(BaseIndicator):
    """Session-reset VWAP.

    Session semantics:
    - ``utc_day`` (default): resets when bar timestamp crosses UTC calendar day.
    """

    def __init__(self, session: str = "utc_day", price_source: str = "typical") -> None:
        super().__init__(name="session_vwap", warmup_bars=1)
        if session != "utc_day":
            raise ValueError("session_vwap currently supports session='utc_day' only")
        if price_source not in {"typical", "close"}:
            raise ValueError("price_source must be 'typical' or 'close'")
        self._session = session
        self._price_source = price_source
        self._session_key: pd.Timestamp | None = None
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    def _resolve_session_key(self, ts: pd.Timestamp) -> pd.Timestamp:
        return ts.floor("D")

    def _price(self, bar: Bar) -> float:
        if self._price_source == "close":
            return bar.close
        return (bar.high + bar.low + bar.close) / 3

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        session_key = self._resolve_session_key(bar.ts)
        if self._session_key is None or session_key != self._session_key:
            self._session_key = session_key
            self._cum_pv = 0.0
            self._cum_vol = 0.0

        if bar.volume > 0:
            px = self._price(bar)
            self._cum_pv += px * bar.volume
            self._cum_vol += bar.volume

    def reset(self) -> None:
        self._bars_seen = 0
        self._session_key = None
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    @property
    def value(self) -> float | None:
        if self._cum_vol <= 0:
            return None
        return safe_div(self._cum_pv, self._cum_vol, default=0.0)


@register("anchored_vwap")
class AnchoredVWAP(BaseIndicator):
    """VWAP anchored at a deterministic index or timestamp.

    Before anchor is reached, output is ``None``.
    """

    def __init__(self, *, anchor_index: int | None = None, anchor_ts: pd.Timestamp | None = None, price_source: str = "typical") -> None:
        super().__init__(name="anchored_vwap", warmup_bars=1)
        if (anchor_index is None) == (anchor_ts is None):
            raise ValueError("provide exactly one of anchor_index or anchor_ts")
        if anchor_index is not None and anchor_index < 0:
            raise ValueError("anchor_index must be >= 0")
        if anchor_ts is not None and anchor_ts.tz is None:
            raise ValueError("anchor_ts must be timezone-aware")
        if price_source not in {"typical", "close"}:
            raise ValueError("price_source must be 'typical' or 'close'")
        self._anchor_index = anchor_index
        self._anchor_ts = anchor_ts
        self._price_source = price_source
        self._bar_index = -1
        self._anchored = False
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    def _price(self, bar: Bar) -> float:
        if self._price_source == "close":
            return bar.close
        return (bar.high + bar.low + bar.close) / 3

    def update(self, bar: Bar) -> None:
        self._bars_seen += 1
        self._bar_index += 1
        if not self._anchored:
            if self._anchor_index is not None and self._bar_index >= self._anchor_index:
                self._anchored = True
            elif self._anchor_ts is not None and bar.ts >= self._anchor_ts:
                self._anchored = True
        if not self._anchored:
            return

        if bar.volume > 0:
            px = self._price(bar)
            self._cum_pv += px * bar.volume
            self._cum_vol += bar.volume

    def reset(self) -> None:
        self._bars_seen = 0
        self._bar_index = -1
        self._anchored = False
        self._cum_pv = 0.0
        self._cum_vol = 0.0

    @property
    def value(self) -> float | None:
        if not self._anchored or self._cum_vol <= 0:
            return None
        return safe_div(self._cum_pv, self._cum_vol, default=0.0)
