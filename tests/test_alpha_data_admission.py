from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bt.institutional.alpha import AlphaAdmissionError, real_data_admission_receipt
from bt.institutional.receipt import verify_receipt
from scripts.build_alpha_data_admission_batch import build_batch


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
            "open": [1.0, 2.0],
            "high": [2.0, 3.0],
            "low": [0.5, 1.5],
            "close": [1.5, 2.5],
            "volume": [10.0, 11.0],
            "quote_volume": [15.0, 27.5],
        }
    ).to_parquet(panel, index=False)
    manifests = root / "manifests"
    manifests.mkdir()
    base = {
        "market": ["perp"],
        "exchange": ["bybit"],
        "symbol": ["BTCUSDT"],
        "dataset": ["ohlcv"],
        "timeframe": ["1m"],
    }
    pd.DataFrame(
        {**base, "expected_rows": [2], "actual_rows": [2], "missing_rows": [0]}
    ).to_parquet(manifests / "coverage.parquet", index=False)
    pd.DataFrame({**base, "status": ["success"], "last_row_count": [2]}).to_parquet(
        manifests / "fetch_state.parquet", index=False
    )
    pd.DataFrame(
        {
            "market": ["perp"],
            "exchange": ["bybit"],
            "native_symbol": ["BTCUSDT"],
            "canonical_symbol": ["BTC-USDT-PERP"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
            "settle_asset": ["USDT"],
            "contract_type": ["PERPETUAL"],
            "status": ["Trading"],
            "first_seen_ts": [timestamps[0]],
            "last_seen_ts": [timestamps[-1]],
            "price_precision": [1],
            "qty_precision": [3],
        }
    ).to_parquet(manifests / "instruments.parquet", index=False)
    return panel


def test_admits_canonical_exchange_history_with_no_authority(tmp_path):
    panel = fixture(tmp_path)
    receipt = real_data_admission_receipt(
        data_root=tmp_path,
        panel_path=panel,
        venue="bybit",
        instrument="BTCUSDT",
        timeframe="1m",
        source_commit=COMMIT,
        backup_root=tmp_path / "backups",
    )
    assert receipt.result["admitted"] is True
    assert receipt.result["schema_version"] == "alpha001-real-data-admission-v2.0.0"
    assert receipt.result["row_count"] == 2
    assert "quote_volume" in receipt.result["output_columns"]
    assert receipt.dataset_digest == receipt.result["panel_sha256"]
    assert verify_receipt(receipt)
    assert not any(receipt.authority.values())
    assert receipt.result["instrument_reference"]["base_asset"] == "BTC"
    assert all(receipt.result["checks"].values())
    assert {
        key: receipt.result["quality"][key]
        for key in (
            "duplicate_timestamp_count",
            "missing_bar_count",
            "out_of_order_timestamp_count",
            "non_finite_value_count",
            "non_positive_price_count",
            "negative_volume_count",
            "invalid_ohlc_geometry_count",
        )
    } == {
        "duplicate_timestamp_count": 0,
        "missing_bar_count": 0,
        "out_of_order_timestamp_count": 0,
        "non_finite_value_count": 0,
        "non_positive_price_count": 0,
        "negative_volume_count": 0,
        "invalid_ohlc_geometry_count": 0,
    }
    recovery = receipt.result["recovery_copy"]
    assert recovery["integrity_verified"] is True
    assert (
        Path(recovery["uri"].removeprefix("file://")).read_bytes() == panel.read_bytes()
    )


def test_rejects_duplicate_or_noncanonical_history(tmp_path):
    panel = fixture(tmp_path, duplicate=True)
    with pytest.raises(AlphaAdmissionError, match="duplicate_timestamps"):
        real_data_admission_receipt(
            data_root=tmp_path,
            panel_path=panel,
            venue="bybit",
            instrument="BTCUSDT",
            timeframe="1m",
            source_commit=COMMIT,
            backup_root=tmp_path / "backups",
        )
    outside = tmp_path.parent / "outside.parquet"
    panel.replace(outside)
    with pytest.raises(AlphaAdmissionError, match="beneath"):
        real_data_admission_receipt(
            data_root=tmp_path,
            panel_path=outside,
            venue="bybit",
            instrument="BTCUSDT",
            timeframe="1m",
            source_commit=COMMIT,
            backup_root=tmp_path / "backups",
        )


def test_batch_admits_only_the_exact_preregistered_assets(tmp_path):
    fixture(tmp_path)
    assignment = {
        "candidate_id": "candidate-1",
        "candidate_digest": "b" * 64,
        "catalog_receipt_digest": "c" * 64,
        "source_commit": COMMIT,
        "data_root": str(tmp_path),
        "backup_root": str(tmp_path / "backups"),
        "assets": [
            {
                "venue": "bybit",
                "instrument": "BTCUSDT",
                "timeframe": "1m",
                "required_fields": ["ts", "close", "quote_volume"],
            }
        ],
    }
    result = build_batch(assignment)
    assert result["candidate_digest"] == "b" * 64
    assert result["catalog_receipt_digest"] == "c" * 64
    assert result["capital_or_order_authority"] is False
    assert [item["result"]["instrument"] for item in result["receipts"]] == [
        "BTCUSDT"
    ]
    assignment["assets"].append(assignment["assets"][0])
    with pytest.raises(ValueError, match="must be unique"):
        build_batch(assignment)


def test_batch_rejects_panel_missing_candidate_required_field(tmp_path):
    panel = fixture(tmp_path)
    frame = pd.read_parquet(panel).drop(columns=["quote_volume"])
    frame.to_parquet(panel, index=False)
    assignment = {
        "candidate_id": "candidate-1",
        "candidate_digest": "b" * 64,
        "catalog_receipt_digest": "c" * 64,
        "source_commit": COMMIT,
        "data_root": str(tmp_path),
        "backup_root": str(tmp_path / "backups"),
        "assets": [
            {
                "venue": "bybit",
                "instrument": "BTCUSDT",
                "timeframe": "1m",
                "required_fields": ["ts", "close", "quote_volume"],
            }
        ],
    }
    with pytest.raises(ValueError, match="missing required fields: quote_volume"):
        build_batch(assignment)


def test_recovery_root_cannot_redirect_through_a_symlink(tmp_path):
    panel = fixture(tmp_path / "lake")
    real_backup = tmp_path / "real-backup"
    real_backup.mkdir()
    linked_backup = tmp_path / "linked-backup"
    linked_backup.symlink_to(real_backup, target_is_directory=True)

    with pytest.raises(AlphaAdmissionError, match="symbolic links"):
        real_data_admission_receipt(
            data_root=tmp_path / "lake",
            panel_path=panel,
            venue="bybit",
            instrument="BTCUSDT",
            timeframe="1m",
            source_commit=COMMIT,
            backup_root=linked_backup,
        )
