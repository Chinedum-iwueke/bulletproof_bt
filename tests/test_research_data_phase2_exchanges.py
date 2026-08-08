from __future__ import annotations

import http.client

import pandas as pd

from bt.research_data.exchanges.binance import BinanceSpotAdapter, BinanceUSDMPerpAdapter
from bt.research_data.exchanges.bybit import BybitSpotAdapter, BybitUSDTPerpAdapter, BybitV5Client, normalize_bybit_instruments
from bt.research_data.exchanges.okx import normalize_okx_instruments
from bt.research_data.instruments import native_to_canonical_symbol
from bt.research_data.schemas import FUNDING_COLUMNS, INSTRUMENT_COLUMNS, OHLCV_COLUMNS


class BinanceInstrumentClient:
    def get(self, path, params=None):
        assert path == "/fapi/v1/exchangeInfo"
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "baseAsset": "BTC",
                    "marginAsset": "USDT",
                    "status": "TRADING",
                    "onboardDate": 1609459200000,
                    "deliveryDate": 4133404800000,
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                }
            ]
        }


class BinanceKlineClient:
    def get(self, path, params=None):
        return [[1609459200000, "1", "2", "0.5", "1.5", "10", 1609459259999, "15", 42]]


class BinanceSpotInstrumentClient:
    def get(self, path, params=None):
        assert path == "/api/v3/exchangeInfo"
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "quoteAsset": "USDT",
                    "baseAsset": "BTC",
                    "status": "TRADING",
                    "permissions": ["SPOT"],
                    "quotePrecision": 2,
                    "baseAssetPrecision": 8,
                }
            ]
        }


class BybitSpotInstrumentClient:
    def get(self, path, params=None):
        assert path == "/v5/market/instruments-info"
        assert params["category"] == "spot"
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "status": "Trading",
                        "priceFilter": {"tickSize": "0.01"},
                        "lotSizeFilter": {"basePrecision": "0.000001"},
                    }
                ],
                "nextPageCursor": "",
            },
        }


class BybitKlineClient:
    def get(self, path, params=None):
        return {
            "retCode": 0,
            "result": {
                "list": [
                    ["1609459200000", "1", "2", "0.5", "1.5", "10", "15"],
                    ["1609459260000", "2", "3", "2.5", "2.1", "10", "15"],
                    ["1609459320000", "2", "3", "1.9", "2.2", "10", "15"],
                ]
            },
        }


def test_bybit_client_retries_remote_disconnected(monkeypatch) -> None:
    calls = {"count": 0}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return b'{"retCode":0,"result":{"list":[]}}'

    def fake_urlopen(_request, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise http.client.RemoteDisconnected("closed before response")
        return Response()

    monkeypatch.setattr("bt.research_data.exchanges.bybit.urllib.request.urlopen", fake_urlopen)

    payload = BybitV5Client(retries=2, backoff_seconds=0).get("/v5/market/instruments-info", {"category": "linear"})

    assert calls["count"] == 2
    assert payload["retCode"] == 0


class BybitFundingClient:
    def get(self, path, params=None):
        assert path == "/v5/market/funding/history"
        return {
            "retCode": 0,
            "result": {
                "list": [
                    {"fundingRateTimestamp": "1609459200250", "fundingRate": "0.0001"},
                    {"fundingRateTimestamp": "1609473600000", "fundingRate": "0.0002"},
                    {"fundingRateTimestamp": "1609488000000", "fundingRate": "0.0003"},
                ]
            },
        }


def test_binance_instrument_payload_normalizes_to_canonical_schema() -> None:
    df = BinanceUSDMPerpAdapter(BinanceInstrumentClient()).fetch_usdt_perp_instruments()

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "exchange"] == "binance"
    assert df.loc[0, "native_symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"
    assert df.loc[0, "settle_asset"] == "USDT"


def test_bybit_instrument_payload_normalizes_to_canonical_schema() -> None:
    df = normalize_bybit_instruments(
        {
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "LinearPerpetual",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "settleCoin": "USDT",
                        "status": "Trading",
                        "launchTime": "1609459200000",
                        "priceFilter": {"tickSize": "0.10"},
                        "lotSizeFilter": {"qtyStep": "0.001"},
                    }
                ]
            }
        }
    )

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "exchange"] == "bybit"
    assert df.loc[0, "native_symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"
    assert df.loc[0, "price_precision"] == 1
    assert df.loc[0, "qty_precision"] == 3


def test_okx_instrument_payload_normalizes_to_canonical_schema() -> None:
    df = normalize_okx_instruments(
        {
            "data": [
                {
                    "instType": "SWAP",
                    "instId": "BTC-USDT-SWAP",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "settleCcy": "USDT",
                    "state": "live",
                    "tickSz": "0.1",
                    "lotSz": "0.01",
                }
            ]
        }
    )

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "exchange"] == "okx"
    assert df.loc[0, "native_symbol"] == "BTC-USDT-SWAP"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"
    assert df.loc[0, "price_precision"] == 1
    assert df.loc[0, "qty_precision"] == 2


def test_binance_spot_instrument_payload_uses_spot_canonical_schema() -> None:
    df = BinanceSpotAdapter(BinanceSpotInstrumentClient()).fetch_spot_instruments()

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "market"] == "spot"
    assert df.loc[0, "exchange"] == "binance"
    assert df.loc[0, "native_symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-SPOT"
    assert df.loc[0, "contract_type"] == "SPOT"


def test_bybit_spot_instrument_payload_uses_spot_canonical_schema() -> None:
    df = BybitSpotAdapter(BybitSpotInstrumentClient()).fetch_spot_instruments()

    assert tuple(df.columns) == INSTRUMENT_COLUMNS
    assert df.loc[0, "market"] == "spot"
    assert df.loc[0, "exchange"] == "bybit"
    assert df.loc[0, "native_symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-SPOT"
    assert df.loc[0, "contract_type"] == "SPOT"


def test_native_symbol_mapping_examples() -> None:
    assert native_to_canonical_symbol("BTCUSDT") == "BTC-USDT-PERP"
    assert native_to_canonical_symbol("BTCUSDT", market="spot") == "BTC-USDT-SPOT"
    assert native_to_canonical_symbol("BTC-USDT-SWAP") == "BTC-USDT-PERP"


def test_binance_ohlcv_includes_native_and_canonical_symbol() -> None:
    df = BinanceUSDMPerpAdapter(BinanceKlineClient()).fetch_ohlcv(
        "BTCUSDT",
        pd.Timestamp("2021-01-01", tz="UTC"),
        pd.Timestamp("2021-01-01 00:02", tz="UTC"),
    )

    assert tuple(df.columns) == OHLCV_COLUMNS
    assert df.loc[0, "symbol"] == "BTCUSDT"
    assert df.loc[0, "canonical_symbol"] == "BTC-USDT-PERP"


def test_bybit_ohlcv_drops_invalid_exchange_candles() -> None:
    df = BybitUSDTPerpAdapter(BybitKlineClient()).fetch_ohlcv(
        "BTCUSDT",
        pd.Timestamp("2021-01-01", tz="UTC"),
        pd.Timestamp("2021-01-01 00:04", tz="UTC"),
    )

    assert tuple(df.columns) == OHLCV_COLUMNS
    assert df["ts"].tolist() == [
        pd.Timestamp("2021-01-01 00:00", tz="UTC"),
        pd.Timestamp("2021-01-01 00:02", tz="UTC"),
    ]


def test_bybit_funding_normalizes_exchange_event_timestamps_without_forcing_8h_grid() -> None:
    df = BybitUSDTPerpAdapter(BybitFundingClient()).fetch_funding(
        "BTCUSDT",
        pd.Timestamp("2021-01-01", tz="UTC"),
        pd.Timestamp("2021-01-01 12:01", tz="UTC"),
    )

    assert tuple(df.columns) == FUNDING_COLUMNS
    assert df["ts"].tolist() == [
        pd.Timestamp("2021-01-01 00:00", tz="UTC"),
        pd.Timestamp("2021-01-01 04:00", tz="UTC"),
        pd.Timestamp("2021-01-01 08:00", tz="UTC"),
    ]
    assert df["funding_rate"].tolist() == [0.0001, 0.0002, 0.0003]
