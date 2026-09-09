from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.institutional.alpha import AlphaAdmissionError, real_data_admission_receipt
from bt.institutional.receipt import verify_receipt


COMMIT = "a" * 40


def fixture(root: Path, *, duplicate: bool = False) -> Path:
    panel = root / "canonical/perp/bybit/BTCUSDT/timeframe=1m/research_panel.parquet"
    panel.parent.mkdir(parents=True)
    timestamps = pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"])
    if duplicate:
        timestamps = pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"])
    pd.DataFrame(
        {
            "ts": timestamps,
            "exchange": ["bybit", "bybit"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "canonical_symbol": ["BTC-USDT-PERP", "BTC-USDT-PERP"],
            "open": [1.0, 2.0], "high": [2.0, 3.0], "low": [0.5, 1.5],
            "close": [1.5, 2.5], "volume": [10.0, 11.0],
        }
    ).to_parquet(panel, index=False)
    manifests = root / "manifests"
    manifests.mkdir()
    base = {
        "market": ["perp"], "exchange": ["bybit"], "symbol": ["BTCUSDT"],
        "dataset": ["ohlcv"], "timeframe": ["1m"],
    }
    pd.DataFrame({**base, "expected_rows": [2], "actual_rows": [2], "missing_rows": [0]}).to_parquet(manifests / "coverage.parquet", index=False)
    pd.DataFrame({**base, "status": ["success"], "last_row_count": [2]}).to_parquet(manifests / "fetch_state.parquet", index=False)
    return panel


def test_admits_canonical_exchange_history_with_no_authority(tmp_path):
    panel = fixture(tmp_path)
    receipt = real_data_admission_receipt(
        data_root=tmp_path, panel_path=panel, venue="bybit", instrument="BTCUSDT",
        timeframe="1m", source_commit=COMMIT,
    )
    assert receipt.result["admitted"] is True
    assert receipt.result["row_count"] == 2
    assert receipt.dataset_digest == receipt.result["panel_sha256"]
    assert verify_receipt(receipt)
    assert not any(receipt.authority.values())


def test_rejects_duplicate_or_noncanonical_history(tmp_path):
    panel = fixture(tmp_path, duplicate=True)
    with pytest.raises(AlphaAdmissionError, match="duplicate_timestamps"):
        real_data_admission_receipt(
            data_root=tmp_path, panel_path=panel, venue="bybit", instrument="BTCUSDT",
            timeframe="1m", source_commit=COMMIT,
        )
    outside = tmp_path.parent / "outside.parquet"
    panel.replace(outside)
    with pytest.raises(AlphaAdmissionError, match="beneath"):
        real_data_admission_receipt(
            data_root=tmp_path, panel_path=outside, venue="bybit", instrument="BTCUSDT",
            timeframe="1m", source_commit=COMMIT,
        )
