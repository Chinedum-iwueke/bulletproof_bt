from __future__ import annotations

import pandas as pd
import pytest

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


def test_online_state_layer_promotes_research_panel_features() -> None:
    layer = OnlineStateFeatureLayer()
    ts0 = pd.Timestamp("2024-01-01T00:00:00Z")
    for i in range(10):
        ts = ts0 + pd.Timedelta(minutes=i)
        layer.update(
            symbol="BTCUSDT",
            ts=ts,
            open_px=100 + i,
            high=101 + i,
            low=99 + i,
            close=100 + i,
            volume=1000 + i,
            extra={
                "mark_close": 100.1 + i,
                "index_close": 100.0 + i,
                "funding_rate": 0.0001 * i,
                "funding_source_ts": ts - pd.Timedelta(hours=8),
                "open_interest": 1_000_000 + i,
                "oi_source_ts": ts,
                "oi_change_1": 10 + i,
                "oi_change_pct_1": 0.001 * i,
                "premium_mark_vs_index": 0.0005 * i,
                "basis_close_vs_index": 0.0003 * i,
                "liq_buy_notional": 100 * i,
                "liq_sell_notional": 50 * i,
            },
        )

    snap = layer.snapshot(symbol="BTCUSDT")

    assert snap["entry_state_mark_close"] == 109.1
    assert snap["entry_state_index_close"] == 109.0
    assert snap["entry_state_funding_rate"] == pytest.approx(0.0009)
    assert snap["entry_state_open_interest"] == 1_000_009
    assert snap["entry_state_oi_change_pct_1"] == pytest.approx(0.009)
    assert snap["entry_state_premium_mark_vs_index"] == pytest.approx(0.0045)
    assert snap["entry_state_basis_close_vs_index"] == pytest.approx(0.0027)
    assert snap["entry_state_liq_buy_notional"] == 900
    assert snap["entry_state_liq_sell_notional"] == 450
    assert snap["entry_state_funding_pctile"] is not None
    assert snap["entry_state_oi_accel_pctile"] is not None


def test_online_state_minimal_profile_omits_perp_feature_windows() -> None:
    layer = OnlineStateFeatureLayer(profile="minimal")
    ts0 = pd.Timestamp("2025-01-01T00:00:00Z")
    for i in range(12):
        layer.update(
            symbol="BTCUSDT",
            ts=ts0 + pd.Timedelta(minutes=i),
            open_px=100 + i,
            high=101 + i,
            low=99 + i,
            close=100 + i,
            volume=1000 + i,
            extra={"funding_rate": 0.1, "open_interest": 1000},
        )

    snap = layer.snapshot(symbol="BTCUSDT")

    assert snap["entry_state_trend_ready"] is True
    assert "entry_state_atr_pct" in snap
    assert "entry_state_funding_rate" not in snap


def test_online_state_layer_uses_precomputed_snapshot_without_recompute() -> None:
    layer = OnlineStateFeatureLayer()
    ts = pd.Timestamp("2025-01-01T00:00:00Z")

    layer.update(
        symbol="BTCUSDT",
        ts=ts,
        open_px=100,
        high=101,
        low=99,
        close=100,
        volume=1000,
        extra={
            "entry_state_csi_raw": 0.77,
            "entry_state_csi_pctile": 0.88,
            "entry_state_csi_source": "enriched",
            "entry_state_funding_pctile": 0.91,
        },
    )

    snap = layer.snapshot(symbol="BTCUSDT")
    assert snap["entry_state_precomputed"] is True
    assert snap["entry_state_csi_raw"] == 0.77
    assert snap["entry_state_csi_pctile"] == 0.88
    assert snap["entry_state_funding_pctile"] == 0.91
