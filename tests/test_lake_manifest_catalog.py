from pathlib import Path

import pandas as pd
import pytest

from bt.institutional.lake_manifest import manifest_catalog_receipt
from bt.institutional.receipt import verify_receipt


def write_manifests(root: Path) -> None:
    manifests = root / "manifests"
    manifests.mkdir(parents=True)
    base = {
        "market": ["perp", "perp"],
        "exchange": ["binance", "bybit"],
        "symbol": ["ETHUSDT", "BTCUSDT"],
        "dataset": ["ohlcv", "ohlcv"],
        "timeframe": ["1m", "1m"],
    }
    pd.DataFrame({
        **base,
        "expected_rows": [10, 10],
        "actual_rows": [10, 9],
        "missing_rows": [0, 1],
        "largest_gap_minutes": [0, 1],
        "first_ts": pd.to_datetime(["2025-01-01T00:00:00Z"] * 2),
        "last_ts": pd.to_datetime(["2025-01-01T00:09:00Z"] * 2),
        "updated_at": pd.to_datetime(["2025-01-02T00:00:00Z"] * 2),
    }).to_parquet(manifests / "coverage.parquet", index=False)
    pd.DataFrame({
        **base,
        "last_successful_ts": pd.to_datetime(["2025-01-01T00:09:00Z"] * 2),
        "last_attempt_ts": pd.to_datetime(["2025-01-02T00:00:00Z"] * 2),
        "last_row_count": [10, 9],
        "status": ["success", "partial"],
        "error_message": [None, "one gap"],
        "updated_at": pd.to_datetime(["2025-01-02T00:00:00Z"] * 2),
    }).to_parquet(manifests / "fetch_state.parquet", index=False)
    pd.DataFrame({
        "market": ["perp", "perp"],
        "exchange": ["binance", "bybit"],
        "native_symbol": ["ETHUSDT", "BTCUSDT"],
        "canonical_symbol": ["ETH-USDT-PERP", "BTC-USDT-PERP"],
        "base_asset": ["ETH", "BTC"],
        "quote_asset": ["USDT", "USDT"],
        "settle_asset": ["USDT", "USDT"],
        "contract_type": ["linear", "linear"],
        "status": ["active", "active"],
        "first_seen_ts": pd.to_datetime(["2024-01-01T00:00:00Z"] * 2),
        "last_seen_ts": pd.to_datetime(["2025-01-02T00:00:00Z"] * 2),
        "price_precision": [2, 2],
        "qty_precision": [3, 3],
    }).to_parquet(manifests / "instruments.parquet", index=False)
    pd.DataFrame({
        "configured_symbol": ["BTCUSDT"], "symbol": ["BTCUSDT"],
        "native_symbol": ["BTCUSDT"], "exchange": ["bybit"],
        "universe": ["stable"], "available": [True],
        "first_seen_ts": pd.to_datetime(["2024-01-01T00:00:00Z"]),
        "created_ts": pd.to_datetime(["2025-01-02T00:00:00Z"]),
        "market": ["perp"],
    }).to_parquet(manifests / "stable_universe.parquet", index=False)


def test_manifest_catalog_is_fast_visibility_not_execution_admission(tmp_path):
    write_manifests(tmp_path)
    receipt = manifest_catalog_receipt(data_root=tmp_path, source_commit="a" * 40)
    assert verify_receipt(receipt)
    assert receipt.result["assets"] == [
        ("perp", "binance", "ETHUSDT"), ("perp", "bybit", "BTCUSDT")
    ]
    assert receipt.result["availability_record_count"] == 2
    assert receipt.result["availability"][1]["fetch_status"] == "partial"
    assert receipt.result["one_year_coverage_candidates"] == []
    assert receipt.result["execution_eligible"] is False
    assert receipt.result["group_labels_are_optional_metadata"] is True
    assert receipt.result["venue_scope"] == ["binance", "bybit"]
    assert not any(receipt.authority.values())


def test_manifest_catalog_requires_native_core_manifests(tmp_path):
    write_manifests(tmp_path)
    (tmp_path / "manifests/instruments.parquet").unlink()
    with pytest.raises(ValueError, match="instruments"):
        manifest_catalog_receipt(data_root=tmp_path, source_commit="a" * 40)


def test_manifest_catalog_exposes_one_year_visibility_without_admission(tmp_path):
    write_manifests(tmp_path)
    path = tmp_path / "manifests/coverage.parquet"
    frame = pd.read_parquet(path)
    frame.loc[0, ["expected_rows", "actual_rows"]] = 525_600
    frame.to_parquet(path, index=False)
    receipt = manifest_catalog_receipt(data_root=tmp_path, source_commit="a" * 40)
    assert receipt.result["one_year_coverage_candidates"] == [{
        "market": "perp", "venue": "binance", "instrument": "ETHUSDT",
        "timeframe": "1m", "actual_rows": 525_600, "missing_rows": 0,
        "first_ts": "2025-01-01T00:00:00Z", "last_ts": "2025-01-01T00:09:00Z",
        "fetch_status": "success", "execution_eligible": False,
    }]


def test_manifest_catalog_rejects_symlinked_manifest(tmp_path):
    write_manifests(tmp_path)
    path = tmp_path / "manifests/coverage.parquet"
    outside = tmp_path / "outside.parquet"
    path.replace(outside)
    path.symlink_to(outside)
    with pytest.raises(ValueError, match="non-symlink"):
        manifest_catalog_receipt(data_root=tmp_path, source_commit="a" * 40)


def test_manifest_catalog_rejects_unapproved_venue_scope(tmp_path):
    write_manifests(tmp_path)
    with pytest.raises(ValueError, match="Binance/Bybit"):
        manifest_catalog_receipt(
            data_root=tmp_path, source_commit="a" * 40, venues=("okx",)
        )
