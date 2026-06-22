from __future__ import annotations

from pathlib import Path
from collections import deque

import pandas as pd
import pytest

from bt.core.errors import DataError
from bt.data.research_panel_loader import _row_to_bar
from bt.features.online_state import OnlineStateFeatureLayer, _pctile
from bt.features.state_builders import _rolling_percentile
from orchestrator.validate_rich_state_integration import main as validate_rich_state_main


def _update(layer: OnlineStateFeatureLayer, i: int, extra: dict | None = None) -> None:
    ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
    layer.update(
        symbol="BTCUSDT",
        ts=ts,
        open_px=100 + i,
        high=101 + i,
        low=99 + i,
        close=100 + i,
        volume=1000 + i,
        extra=extra or {},
    )


def test_ohlcv_only_state_uses_proxy_csi() -> None:
    layer = OnlineStateFeatureLayer()
    for i in range(12):
        _update(layer, i)

    snap = layer.snapshot(symbol="BTCUSDT")

    assert snap["entry_state_csi_source"] == "ohlcv_proxy"
    assert snap["entry_state_funding_raw"] is None


def test_constant_values_have_neutral_causal_percentiles() -> None:
    rolled = _rolling_percentile(pd.Series([0.0] * 8), window=8)
    assert rolled.iloc[-1] == pytest.approx(0.5)
    assert _pctile(deque([0.0] * 8), 0.0) == pytest.approx(0.5)


def test_enriched_state_emits_funding_oi_basis_and_enriched_csi() -> None:
    layer = OnlineStateFeatureLayer()
    for i in range(12):
        ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
        _update(
            layer,
            i,
            {
                "funding_rate": -0.001 + i * 0.0002,
                "funding_available_at": ts,
                "open_interest": 1_000_000 + i * 1000,
                "oi_available_at": ts,
                "mark_close": 100.2 + i,
                "index_close": 100 + i,
                "basis_close_vs_index": 0.002 + i * 0.0001,
            },
        )

    snap = layer.snapshot(symbol="BTCUSDT")

    assert snap["entry_state_csi_source"] == "enriched"
    assert snap["entry_state_funding_pctile"] is not None
    assert snap["entry_state_oi_accel_pctile"] is not None
    assert snap["entry_state_basis_pctile"] is not None
    assert snap["entry_state_mark_price"] is not None
    assert snap["entry_state_index_price"] is not None


def test_missing_rich_components_are_skipped_and_csi_still_computes() -> None:
    layer = OnlineStateFeatureLayer()
    for i in range(12):
        ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
        _update(layer, i, {"funding": i * 0.0001, "funding_available_at": ts})

    snap = layer.snapshot(symbol="BTCUSDT")

    assert snap["entry_state_csi_source"] == "enriched"
    assert 0.0 <= snap["entry_state_csi_raw"] <= 1.0
    assert "funding_extreme_score" in snap["entry_state_csi_components_json"]


def test_available_at_after_bar_is_rejected_by_research_panel_loader() -> None:
    ts = pd.Timestamp("2025-01-01T00:00:00Z")
    payload = {
        "ts": ts,
        "symbol": "BTCUSDT",
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100,
        "volume": 1,
        "funding_rate": 0.001,
        "funding_available_at": ts + pd.Timedelta(minutes=1),
    }

    with pytest.raises(DataError):
        _row_to_bar(payload, expected_symbol="BTCUSDT", path=Path("panel.parquet"), last_ts=None)


def test_rich_state_validator_writes_reports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "data"
    panel_dir = root / "canonical" / "binance" / "BTCUSDT" / "timeframe=1m"
    panel_dir.mkdir(parents=True)
    rows = []
    for i in range(12):
        ts = pd.Timestamp("2025-01-01T00:00:00Z") + pd.Timedelta(minutes=i)
        rows.append(
            {
                "ts": ts,
                "symbol": "BTCUSDT",
                "open": 100 + i,
                "high": 101 + i,
                "low": 99 + i,
                "close": 100 + i,
                "volume": 1000 + i,
                "funding_rate": i * 0.0001,
                "funding_available_at": ts,
                "open_interest": 1_000_000 + i * 1000,
                "oi_available_at": ts,
                "mark_close": 100.2 + i,
                "index_close": 100 + i,
                "basis_close_vs_index": i * 0.0001,
            }
        )
    pd.DataFrame(rows).to_parquet(panel_dir / "research_panel.parquet", index=False)
    out = tmp_path / "audits"

    monkeypatch.setattr(
        "sys.argv",
        [
            "validate_rich_state_integration.py",
            "--data",
            str(root),
            "--output-dir",
            str(out),
            "--sample-symbol",
            "BTCUSDT",
        ],
    )

    assert validate_rich_state_main() == 0
    assert (out / "RICH_STATE_INTEGRATION_AUDIT.md").exists()
    assert (out / "RICH_STATE_INTEGRATION_AUDIT.json").exists()
    assert (out / "rich_state_feature_sample.csv").exists()
