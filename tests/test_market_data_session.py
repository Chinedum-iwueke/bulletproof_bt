from __future__ import annotations

import numpy as np
import pandas as pd

from bt.engine.fast_path.data_session import DataSession


def _panel(root, symbol: str, closes: list[float]) -> pd.DataFrame:
    ts = pd.date_range(pd.Timestamp("2021-01-01 00:00", tz="UTC"), periods=len(closes), freq="1min")
    frame = pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": symbol,
            "open": closes,
            "high": [value + 0.1 for value in closes],
            "low": [value - 0.1 for value in closes],
            "close": closes,
            "volume": [10.0] * len(closes),
            "mark_close": closes,
            "index_close": closes,
            "funding_rate": [0.0001] * len(closes),
            "open_interest": [1000.0 + idx for idx in range(len(closes))],
            "htf_15m_ready": [False, True, False][: len(closes)],
            "htf_15m_close": closes,
        }
    )
    path = root / "canonical" / "binance" / symbol / "timeframe=1m" / "research_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return frame


def test_market_data_session_loads_stable_research_panels_once_as_arrays(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0])
    _panel(root, "ETHUSDT", [4.0, 5.0, 6.0])
    manifest = pd.DataFrame(
        {
            "exchange": ["binance", "binance"],
            "native_symbol": ["BTCUSDT", "ETHUSDT"],
            "available": [True, True],
        }
    )
    manifest_path = root / "manifests" / "stable_universe.parquet"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_parquet(manifest_path, index=False)

    session = DataSession.from_config(
        {
            "data": {
                "dataset_kind": "research_panel",
                "root": str(root),
                "exchange": "binance",
                "universe": "stable",
                "stable_manifest": str(manifest_path),
                "timeframe": "1m",
                "extra_column_prefixes": ["htf_15m_"],
            }
        }
    )
    snapshot = session.snapshot()

    assert snapshot.symbols == ("BTCUSDT", "ETHUSDT")
    assert snapshot.symbol_to_id["BTCUSDT"] == 0
    arrays = snapshot.arrays_for_symbol("BTCUSDT")
    assert arrays.symbol_id == 0
    assert arrays.close.flags["C_CONTIGUOUS"]
    assert arrays.close.tolist() == [1.0, 2.0, 3.0]
    assert arrays.active_mask.tolist() == [True, True, True]
    assert arrays.candidate_ready.tolist() == [False, True, False]
    assert "mark_close" in arrays.extras
    assert "htf_15m_close" in arrays.extras
    assert snapshot.to_json()["candidate_row_count"] == 2


def test_market_data_session_builds_volatile_active_membership_masks(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0])
    _panel(root, "ETHUSDT", [4.0, 5.0, 6.0])
    membership = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 00:01", tz="UTC"),
            ],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "universe": ["volatile", "volatile"],
        }
    )
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)

    session = DataSession.from_config(
        {
            "data": {
                "dataset_kind": "research_panel",
                "root": str(root),
                "exchange": "binance",
                "universe": "volatile",
                "membership_path": str(membership_path),
                "timeframe": "1m",
                "date_range": {"start": "2021-01-01T00:00:00Z", "end": "2021-01-01T00:03:00Z"},
            }
        }
    )
    snapshot = session.snapshot()

    btc = snapshot.arrays_for_symbol("BTCUSDT")
    eth = snapshot.arrays_for_symbol("ETHUSDT")
    assert btc.active_mask.tolist() == [True, False, False]
    assert eth.active_mask.tolist() == [False, True, True]
    ts0 = np.int64(pd.Timestamp("2021-01-01 00:00", tz="UTC").value)
    ts1 = np.int64(pd.Timestamp("2021-01-01 00:01", tz="UTC").value)
    assert snapshot.active_symbols_at_ns(ts0) == ("BTCUSDT",)
    assert snapshot.active_symbols_at_ns(ts1) == ("ETHUSDT",)
