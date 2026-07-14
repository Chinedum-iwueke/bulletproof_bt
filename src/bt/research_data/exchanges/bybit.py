"""Bybit V5 linear USDT perpetual REST adapter."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bt.research_data.config import BYBIT_V5_BASE_URL
from bt.research_data.instruments import native_to_canonical_symbol, normalize_instrument_frame
from bt.research_data.schemas import FUNDING_COLUMNS, INDEX_COLUMNS, MARK_COLUMNS, OHLCV_COLUMNS, OI_COLUMNS
from bt.research_data.time import ms, utc_ts


def normalize_bybit_event_ts(value: object) -> pd.Timestamp:
    """Normalize Bybit event timestamps without changing event ordering.

    Bybit funding timestamps are exchange-published event times, and some
    instruments use non-8h funding schedules. We preserve the published instant
    but snap harmless sub-second transport jitter to exact seconds so downstream
    validators can treat the row as a clean causal event.
    """

    ts = pd.to_datetime(int(value), unit="ms", utc=True)
    rounded = ts.round("s")
    if abs(ts - rounded) <= pd.Timedelta(milliseconds=500):
        return rounded
    return ts


@dataclass
class BybitV5Client:
    base_url: str = BYBIT_V5_BASE_URL
    timeout: float = 30.0
    retries: int = 5
    backoff_seconds: float = 0.5

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urllib.parse.urlencode(params or {})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": "bulletproof-bt-research-data/1"})
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if str(payload.get("retCode", "0")) != "0":
                    raise RuntimeError(f"Bybit error {payload.get('retCode')}: {payload.get('retMsg')}")
                return payload
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, RuntimeError) as exc:
                last_error = exc
                if isinstance(exc, urllib.error.HTTPError) and exc.code not in {429, 500, 502, 503, 504}:
                    raise
                time.sleep(self.backoff_seconds * (2**attempt))
        raise RuntimeError(f"Bybit REST request failed after {self.retries} retries: {url}") from last_error


def normalize_bybit_instruments(payload: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for item in payload.get("result", {}).get("list", []):
        if item.get("quoteCoin") != "USDT" or item.get("contractType") not in {"LinearPerpetual", "PERPETUAL"}:
            continue
        rows.append(
            {
                "exchange": "bybit",
                "native_symbol": item.get("symbol"),
                "canonical_symbol": native_to_canonical_symbol(str(item.get("symbol", ""))),
                "base_asset": item.get("baseCoin"),
                "quote_asset": item.get("quoteCoin"),
                "settle_asset": item.get("settleCoin"),
                "contract_type": "PERPETUAL",
                "status": item.get("status"),
                "first_seen_ts": pd.to_datetime(
                    pd.to_numeric(item.get("launchTime"), errors="coerce"),
                    unit="ms",
                    utc=True,
                    errors="coerce",
                ),
                "last_seen_ts": pd.Timestamp.now(tz="UTC"),
                "tick_size": item.get("priceFilter", {}).get("tickSize"),
                "qty_step": item.get("lotSizeFilter", {}).get("qtyStep"),
            }
        )
    return normalize_instrument_frame(rows)


class BybitUSDTPerpAdapter:
    exchange = "bybit"

    def __init__(self, client: BybitV5Client | None = None) -> None:
        self.client = client or BybitV5Client()

    def fetch_usdt_perp_instruments(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            payload = self.client.get("/v5/market/instruments-info", params)
            rows.extend(payload.get("result", {}).get("list", []))
            cursor = payload.get("result", {}).get("nextPageCursor") or ""
            if not cursor:
                break
        return normalize_bybit_instruments({"result": {"list": rows}}).sort_values("native_symbol").reset_index(drop=True)

    def _fetch_kline(self, path: str, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str) -> list[list[Any]]:
        interval = timeframe.removesuffix("m") if timeframe.endswith("m") else timeframe
        rows: list[list[Any]] = []
        cursor_end = utc_ts(end)
        start_ts = utc_ts(start)
        end_ts = utc_ts(end)
        while cursor_end > start_ts:
            payload = self.client.get(
                path,
                {
                    "category": "linear",
                    "symbol": symbol,
                    "interval": interval,
                    "start": ms(start_ts),
                    "end": ms(cursor_end),
                    "limit": 1000,
                },
            )
            chunk = payload.get("result", {}).get("list", [])
            if not chunk:
                break
            rows.extend(chunk)
            oldest = min(pd.to_datetime(int(row[0]), unit="ms", utc=True) for row in chunk)
            if oldest <= start_ts or oldest >= cursor_end:
                break
            cursor_end = oldest
            if len(chunk) < 1000:
                break
        deduped = {
            int(row[0]): row
            for row in rows
            if start_ts <= pd.to_datetime(int(row[0]), unit="ms", utc=True) < end_ts
        }
        return [deduped[key] for key in sorted(deduped)]

    def fetch_ohlcv(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_kline("/v5/market/kline", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "quote_volume": float(row[6]),
                    "trade_count": pd.NA,
                }
                for row in rows
            ],
            columns=OHLCV_COLUMNS,
        )
        if not df.empty:
            valid = df["high"].ge(df[["open", "close", "low"]].max(axis=1)) & df["low"].le(
                df[["open", "close", "high"]].min(axis=1)
            )
            df = df.loc[valid]
        return df[df["ts"] < utc_ts(end)].reset_index(drop=True)

    def fetch_mark(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_kline("/v5/market/mark-price-kline", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "mark_open": float(row[1]),
                    "mark_high": float(row[2]),
                    "mark_low": float(row[3]),
                    "mark_close": float(row[4]),
                }
                for row in rows
            ],
            columns=MARK_COLUMNS,
        )
        return df[df["ts"] < utc_ts(end)].reset_index(drop=True)

    def fetch_index(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_kline("/v5/market/index-price-kline", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "index_open": float(row[1]),
                    "index_high": float(row[2]),
                    "index_low": float(row[3]),
                    "index_close": float(row[4]),
                }
                for row in rows
            ],
            columns=INDEX_COLUMNS,
        )
        return df[df["ts"] < utc_ts(end)].reset_index(drop=True)

    def fetch_funding(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        cursor = utc_ts(start)
        end_ts = utc_ts(end)
        while cursor < end_ts:
            payload = self.client.get(
                "/v5/market/funding/history",
                {"category": "linear", "symbol": symbol, "startTime": ms(cursor), "endTime": ms(end_ts), "limit": 200},
            )
            chunk = payload.get("result", {}).get("list", [])
            if not chunk:
                break
            rows.extend(chunk)
            latest = max(normalize_bybit_event_ts(row["fundingRateTimestamp"]) for row in chunk)
            cursor = latest + pd.Timedelta(milliseconds=1)
            if len(chunk) < 200:
                break
        df = pd.DataFrame(
            [
                {
                    "ts": normalize_bybit_event_ts(row["fundingRateTimestamp"]),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "funding_rate": float(row["fundingRate"]),
                    "mark_price_at_funding": pd.NA,
                }
                for row in rows
            ],
            columns=FUNDING_COLUMNS,
        ).sort_values("ts")
        if not df.empty:
            df = df.drop_duplicates(["exchange", "symbol", "ts"], keep="last")
        return df[df["ts"] < end_ts].reset_index(drop=True)

    def fetch_open_interest(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        cursor = utc_ts(start)
        end_ts = utc_ts(end)
        while cursor < end_ts:
            payload = self.client.get(
                "/v5/market/open-interest",
                {"category": "linear", "symbol": symbol, "intervalTime": "5min", "startTime": ms(cursor), "endTime": ms(end_ts), "limit": 200},
            )
            chunk = payload.get("result", {}).get("list", [])
            if not chunk:
                break
            rows.extend(chunk)
            latest = max(pd.to_datetime(int(row["timestamp"]), unit="ms", utc=True) for row in chunk)
            cursor = latest + pd.Timedelta(milliseconds=1)
            if len(chunk) < 200:
                break
        deduped = {int(row["timestamp"]): row for row in rows}
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(timestamp, unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "open_interest": float(row["openInterest"]),
                    "open_interest_value": pd.NA,
                }
                for timestamp, row in sorted(deduped.items())
            ],
            columns=OI_COLUMNS,
        ).sort_values("ts")
        return df[df["ts"] < end_ts].reset_index(drop=True)


class BybitSpotAdapter:
    exchange = "bybit"
    market = "spot"

    def __init__(self, client: BybitV5Client | None = None) -> None:
        self.client = client or BybitV5Client()

    def fetch_spot_instruments(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params = {"category": "spot", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            payload = self.client.get("/v5/market/instruments-info", params)
            rows.extend(payload.get("result", {}).get("list", []))
            cursor = payload.get("result", {}).get("nextPageCursor") or ""
            if not cursor:
                break
        normalized = []
        for item in rows:
            if item.get("quoteCoin") != "USDT":
                continue
            normalized.append(
                {
                    "market": "spot",
                    "exchange": self.exchange,
                    "native_symbol": item.get("symbol"),
                    "canonical_symbol": native_to_canonical_symbol(str(item.get("symbol", "")), market="spot"),
                    "base_asset": item.get("baseCoin"),
                    "quote_asset": item.get("quoteCoin"),
                    "settle_asset": item.get("quoteCoin"),
                    "contract_type": "SPOT",
                    "status": item.get("status"),
                    "first_seen_ts": pd.to_datetime(
                        pd.to_numeric(item.get("launchTime"), errors="coerce"),
                        unit="ms",
                        utc=True,
                        errors="coerce",
                    ),
                    "last_seen_ts": pd.Timestamp.now(tz="UTC"),
                    "tick_size": item.get("priceFilter", {}).get("tickSize"),
                    "qty_step": item.get("lotSizeFilter", {}).get("basePrecision"),
                }
            )
        return normalize_instrument_frame(normalized).sort_values("native_symbol").reset_index(drop=True)

    def _fetch_spot_kline(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str) -> list[list[Any]]:
        interval = timeframe.removesuffix("m") if timeframe.endswith("m") else timeframe
        rows: list[list[Any]] = []
        cursor_end = utc_ts(end)
        start_ts = utc_ts(start)
        end_ts = utc_ts(end)
        while cursor_end > start_ts:
            payload = self.client.get(
                "/v5/market/kline",
                {
                    "category": "spot",
                    "symbol": symbol,
                    "interval": interval,
                    "start": ms(start_ts),
                    "end": ms(cursor_end),
                    "limit": 1000,
                },
            )
            chunk = payload.get("result", {}).get("list", [])
            if not chunk:
                break
            rows.extend(chunk)
            oldest = min(pd.to_datetime(int(row[0]), unit="ms", utc=True) for row in chunk)
            if oldest <= start_ts or oldest >= cursor_end:
                break
            cursor_end = oldest
            if len(chunk) < 1000:
                break
        deduped = {
            int(row[0]): row
            for row in rows
            if start_ts <= pd.to_datetime(int(row[0]), unit="ms", utc=True) < end_ts
        }
        return [deduped[key] for key in sorted(deduped)]

    def fetch_spot_ohlcv(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_spot_kline(symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol, market="spot"),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "quote_volume": float(row[6]),
                    "trade_count": pd.NA,
                }
                for row in rows
            ],
            columns=OHLCV_COLUMNS,
        )
        if not df.empty:
            valid = df["high"].ge(df[["open", "close", "low"]].max(axis=1)) & df["low"].le(
                df[["open", "close", "high"]].min(axis=1)
            )
            df = df.loc[valid]
        return df[df["ts"] < utc_ts(end)].reset_index(drop=True)
