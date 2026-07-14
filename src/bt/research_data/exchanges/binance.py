"""Native Binance USD-M futures REST adapter."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import pandas as pd

from bt.research_data.config import BINANCE_FAPI_BASE_URL, BINANCE_SPOT_BASE_URL
from bt.research_data.instruments import native_to_canonical_symbol, normalize_instrument_frame
from bt.research_data.schemas import FUNDING_COLUMNS, INDEX_COLUMNS, MARK_COLUMNS, OHLCV_COLUMNS, OI_COLUMNS
from bt.research_data.time import ms, utc_series, utc_ts


@dataclass
class BinanceFuturesClient:
    base_url: str = BINANCE_FAPI_BASE_URL
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
                    return json.loads(response.read().decode("utf-8"))
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
                if isinstance(exc, urllib.error.HTTPError) and exc.code not in {418, 429, 500, 502, 503, 504}:
                    raise
                sleep_for = self.backoff_seconds * (2**attempt)
                time.sleep(sleep_for)
        raise RuntimeError(f"Binance REST request failed after {self.retries} retries: {url}") from last_error


class BinanceUSDMPerpAdapter:
    exchange = "binance"

    def __init__(self, client: BinanceFuturesClient | None = None) -> None:
        self.client = client or BinanceFuturesClient()

    def fetch_usdt_perp_instruments(self) -> pd.DataFrame:
        payload = self.client.get("/fapi/v1/exchangeInfo")
        rows = []
        for item in payload.get("symbols", []):
            if item.get("contractType") != "PERPETUAL" or item.get("quoteAsset") != "USDT":
                continue
            rows.append(
                {
                    "exchange": self.exchange,
                    "native_symbol": item["symbol"],
                    "canonical_symbol": native_to_canonical_symbol(item["symbol"]),
                    "base_asset": item.get("baseAsset"),
                    "quote_asset": item.get("quoteAsset"),
                    "settle_asset": item.get("marginAsset"),
                    "status": item.get("status"),
                    "contract_type": item.get("contractType"),
                    "first_seen_ts": pd.to_datetime(item.get("onboardDate"), unit="ms", utc=True, errors="coerce"),
                    "last_seen_ts": pd.Timestamp.now(tz="UTC"),
                    "price_precision": item.get("pricePrecision"),
                    "qty_precision": item.get("quantityPrecision"),
                }
            )
        return normalize_instrument_frame(rows).sort_values("native_symbol").reset_index(drop=True)

    def _fetch_klines(
        self,
        path: str,
        symbol: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        timeframe: str,
        symbol_param: str = "symbol",
    ) -> list[list[Any]]:
        start_ts = utc_ts(start)
        end_ts = utc_ts(end)
        rows: list[list[Any]] = []
        cursor = start_ts
        step_ms = 60_000 if timeframe == "1m" else 60_000
        while cursor < end_ts:
            params = {
                symbol_param: symbol,
                "interval": timeframe,
                "startTime": ms(cursor),
                "endTime": ms(end_ts),
                "limit": 1500,
            }
            chunk = self.client.get(path, params)
            if not chunk:
                break
            rows.extend(chunk)
            last_open_ms = int(chunk[-1][0])
            next_ms = last_open_ms + step_ms
            if next_ms <= ms(cursor):
                break
            cursor = pd.to_datetime(next_ms, unit="ms", utc=True)
            if len(chunk) < 1500:
                break
        return rows

    def fetch_ohlcv(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_klines("/fapi/v1/klines", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "quote_volume": float(row[7]),
                    "trade_count": int(row[8]),
                }
                for row in rows
            ],
            columns=OHLCV_COLUMNS,
        )
        return df[df["ts"] < utc_ts(end)].reset_index(drop=True)

    def fetch_mark(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_klines("/fapi/v1/markPriceKlines", symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
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
        pair = symbol.removesuffix("USDT") + "USDT"
        rows = self._fetch_klines("/fapi/v1/indexPriceKlines", pair, start, end, timeframe, symbol_param="pair")
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
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
            chunk = self.client.get(
                "/fapi/v1/fundingRate",
                {"symbol": symbol, "startTime": ms(cursor), "endTime": ms(end_ts), "limit": 1000},
            )
            if not chunk:
                break
            rows.extend(chunk)
            last = pd.to_datetime(int(chunk[-1]["fundingTime"]), unit="ms", utc=True)
            cursor = last + pd.Timedelta(milliseconds=1)
            if len(chunk) < 1000:
                break
        def optional_float(value: object) -> float | object:
            if value in (None, ""):
                return pd.NA
            return float(value)

        def funding_timestamp(value: object) -> pd.Timestamp:
            ts = pd.to_datetime(value, unit="ms", utc=True)
            return ts.round("8h")

        df = pd.DataFrame(
            [
                {
                    "ts": funding_timestamp(row["fundingTime"]),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "funding_rate": float(row["fundingRate"]),
                    "mark_price_at_funding": optional_float(row.get("markPrice")),
                }
                for row in rows
            ],
            columns=FUNDING_COLUMNS,
        )
        if not df.empty:
            df = df.sort_values("ts").drop_duplicates(["exchange", "symbol", "ts"], keep="last")
        return df[df["ts"] < end_ts].reset_index(drop=True)

    def fetch_open_interest(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        """Fetch historical open interest.

        Binance USD-M exposes historical open interest as 5m minimum granularity
        through /futures/data/openInterestHist. True 1m historical OI is not
        available from this endpoint, so canonical panels use backward as-of joins
        from the 5m/event timestamps without interpolation. Binance currently
        returns HTTP 400 for old windows outside the public retention period;
        those windows are represented as unavailable instead of failed data.
        """
        rows: list[dict[str, Any]] = []
        cursor = utc_ts(start)
        end_ts = utc_ts(end)
        retention_start = utc_ts("now") - pd.Timedelta(days=30)
        if end_ts < retention_start:
            return pd.DataFrame(columns=OI_COLUMNS)
        cursor = max(cursor, retention_start)
        while cursor < end_ts:
            try:
                chunk = self.client.get(
                    "/futures/data/openInterestHist",
                    {
                        "symbol": symbol,
                        "period": "5m",
                        "startTime": ms(cursor),
                        "endTime": ms(end_ts),
                        "limit": 500,
                    },
                )
            except urllib.error.HTTPError as exc:
                if exc.code == 400:
                    break
                raise
            if not chunk:
                break
            rows.extend(chunk)
            last = pd.to_datetime(int(chunk[-1]["timestamp"]), unit="ms", utc=True)
            cursor = last + pd.Timedelta(milliseconds=1)
            if len(chunk) < 500:
                break
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(int(row["timestamp"]), unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol),
                    "open_interest": float(row["sumOpenInterest"]),
                    "open_interest_value": float(row["sumOpenInterestValue"]),
                }
                for row in rows
            ],
            columns=OI_COLUMNS,
        )
        return df[df["ts"] < end_ts].reset_index(drop=True)


def get_adapter(exchange: str) -> BinanceUSDMPerpAdapter:
    if exchange != "binance":
        raise ValueError("Phase 1 supports only Binance")
    return BinanceUSDMPerpAdapter()


@dataclass
class BinanceSpotClient(BinanceFuturesClient):
    base_url: str = BINANCE_SPOT_BASE_URL


class BinanceSpotAdapter:
    exchange = "binance"
    market = "spot"

    def __init__(self, client: BinanceSpotClient | None = None) -> None:
        self.client = client or BinanceSpotClient()

    def fetch_spot_instruments(self) -> pd.DataFrame:
        payload = self.client.get("/api/v3/exchangeInfo")
        rows = []
        for item in payload.get("symbols", []):
            if item.get("quoteAsset") != "USDT":
                continue
            permissions = {str(value).upper() for value in item.get("permissions", [])}
            if permissions and "SPOT" not in permissions:
                continue
            rows.append(
                {
                    "market": "spot",
                    "exchange": self.exchange,
                    "native_symbol": item["symbol"],
                    "canonical_symbol": native_to_canonical_symbol(item["symbol"], market="spot"),
                    "base_asset": item.get("baseAsset"),
                    "quote_asset": item.get("quoteAsset"),
                    "settle_asset": item.get("quoteAsset"),
                    "status": item.get("status"),
                    "contract_type": "SPOT",
                    "first_seen_ts": pd.NaT,
                    "last_seen_ts": pd.Timestamp.now(tz="UTC"),
                    "price_precision": item.get("quotePrecision") or item.get("pricePrecision"),
                    "qty_precision": item.get("baseAssetPrecision"),
                }
            )
        return normalize_instrument_frame(rows).sort_values("native_symbol").reset_index(drop=True)

    def _fetch_klines(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str) -> list[list[Any]]:
        start_ts = utc_ts(start)
        end_ts = utc_ts(end)
        rows: list[list[Any]] = []
        cursor = start_ts
        step_ms = 60_000 if timeframe == "1m" else 60_000
        while cursor < end_ts:
            chunk = self.client.get(
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": timeframe,
                    "startTime": ms(cursor),
                    "endTime": ms(end_ts),
                    "limit": 1000,
                },
            )
            if not chunk:
                break
            rows.extend(chunk)
            last_open_ms = int(chunk[-1][0])
            next_ms = last_open_ms + step_ms
            if next_ms <= ms(cursor):
                break
            cursor = pd.to_datetime(next_ms, unit="ms", utc=True)
            if len(chunk) < 1000:
                break
        return rows

    def fetch_spot_ohlcv(self, symbol: str, start: pd.Timestamp, end: pd.Timestamp, timeframe: str = "1m") -> pd.DataFrame:
        rows = self._fetch_klines(symbol, start, end, timeframe)
        df = pd.DataFrame(
            [
                {
                    "ts": pd.to_datetime(row[0], unit="ms", utc=True),
                    "exchange": self.exchange,
                    "symbol": symbol,
                    "canonical_symbol": native_to_canonical_symbol(symbol, market="spot"),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "quote_volume": float(row[7]),
                    "trade_count": int(row[8]),
                }
                for row in rows
            ],
            columns=OHLCV_COLUMNS,
        )
        return df[df["ts"] < utc_ts(end)].reset_index(drop=True)
