from __future__ import annotations

import pandas as pd

from bt.features.state_builders import build_state_features


def test_state_features_are_causal_and_past_only() -> None:
    ts = pd.date_range("2024-01-01", periods=20, freq="15min", tz="UTC")
    bars = pd.DataFrame({"ts": ts, "open": range(20), "high": [x + 1 for x in range(20)], "low": range(20), "close": [x + 0.5 for x in range(20)], "volume": [100 + x for x in range(20)]})
    out1 = build_state_features(bars.iloc[:10], symbol="BTCUSDT")
    out2 = build_state_features(bars.iloc[:20], symbol="BTCUSDT")
    assert out1.iloc[-1]["entry_state_csi_raw"] == out2.iloc[9]["entry_state_csi_raw"]


def test_csi_proxy_computed_from_pctiles() -> None:
    ts = pd.date_range("2024-01-01", periods=220, freq="15min", tz="UTC")
    bars = pd.DataFrame({"ts": ts, "open": 100.0, "high": 101.0, "low": 99.0, "close": [100 + (i % 7) for i in range(220)], "volume": [1000 + i for i in range(220)]})
    out = build_state_features(bars, symbol="ETHUSDT")
    assert out["entry_state_csi_raw"].notna().any()
