from __future__ import annotations

import pandas as pd

from bt.features.online_state import OnlineStateFeatureLayer


def test_online_state_layer_emits_entry_state_snapshot() -> None:
    layer = OnlineStateFeatureLayer()
    ts0 = pd.Timestamp("2024-01-01T00:00:00Z")
    for i in range(30):
        ts = ts0 + pd.Timedelta(minutes=15 * i)
        px = 100 + i
        layer.update(symbol="BTCUSDT", ts=ts, open_px=px - 0.2, high=px + 0.5, low=px - 0.5, close=px, volume=1000 + i)
    snap = layer.snapshot(symbol="BTCUSDT")
    assert snap["entry_state_trend_ready"] is True
    assert snap["entry_state_csi_raw"] is not None
    assert snap["entry_state_vol_regime"] in {"vol_low", "vol_mid", "vol_high", "vol_extreme"}
