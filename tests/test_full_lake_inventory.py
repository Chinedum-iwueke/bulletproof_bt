from pathlib import Path
import json
import runpy
import os
import time

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

import pandas as pd

from bt.institutional.lake_inventory import full_lake_inventory_receipt
from bt.institutional.receipt import verify_receipt


def test_inventory_fails_instead_of_omitting_unreadable_subtree(tmp_path, monkeypatch):
    (tmp_path / "raw").mkdir()
    def inaccessible(path, **options):
        options["onerror"](PermissionError("unreadable subtree"))
        yield
    monkeypatch.setattr("bt.institutional.lake_inventory.os.walk", inaccessible)
    with pytest.raises(PermissionError):
        full_lake_inventory_receipt(data_root=tmp_path, source_commit="a" * 40)


def test_inventory_quarantines_fifo_without_reading_it(tmp_path):
    (tmp_path / "raw").mkdir()
    os.mkfifo(tmp_path / "raw/blocked.parquet")
    item = full_lake_inventory_receipt(data_root=tmp_path, source_commit="a" * 40).result["objects"][0]
    assert item["disposition"] == "quarantined"
    assert "content_digest" not in item


def test_nested_columns_do_not_corrupt_timestamp_bounds(tmp_path):
    from bt.institutional.lake_inventory import _bounds
    path = tmp_path / "nested.parquet"
    dates = pd.date_range("2025-01-01", periods=2, freq="min", tz="UTC")
    pq.write_table(pa.table({"nested": pa.array([{"a": 11, "b": 222}] * 2), "ts": pa.array(dates)}), path)
    result = _bounds(pq.ParquetFile(path))
    assert result["observed_start"].startswith("2025-01-01T00:00:00")
    assert result["observed_end"].startswith("2025-01-01T00:01:00")
    assert "UTC" in result["timestamp_type"]


def test_replaced_path_does_not_rebind_hashed_file_metadata(tmp_path, monkeypatch):
    from bt.institutional import lake_inventory
    root = tmp_path / "lake"
    path = root / "canonical/perp/bybit/ETHUSDT/timeframe=1m/research_panel.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"ts": pd.date_range("2025-01-01", periods=2, freq="min", tz="UTC")}).to_parquet(path)
    outside = tmp_path / "outside.parquet"
    pd.DataFrame({"wrong": [999]}).to_parquet(outside)
    original = lake_inventory._sha256
    def replace_after_hash(handle):
        result = original(handle)
        path.rename(tmp_path / "original.parquet")
        path.symlink_to(outside)
        return result
    monkeypatch.setattr(lake_inventory, "_sha256", replace_after_hash)
    item = full_lake_inventory_receipt(data_root=root, source_commit="a" * 40).result["objects"][0]
    assert item["disposition"] == "quarantined"
    assert "path_replaced_during_inventory" in item["reason_codes"]
    assert item["output_columns"] == ["ts"]


def test_rewrite_with_restored_mtime_is_not_accepted(tmp_path, monkeypatch):
    from bt.institutional import lake_inventory
    path = tmp_path / "canonical/perp/bybit/ETHUSDT/timeframe=1m/research_panel.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame({"ts": pd.date_range("2025-01-01", periods=2, freq="min", tz="UTC")}).to_parquet(path)
    original = lake_inventory._sha256
    def rewrite_after_hash(handle):
        value = original(handle)
        prior = path.stat()
        contents = path.read_bytes()
        time.sleep(0.02)
        path.write_bytes(contents)
        os.utime(path, ns=(prior.st_atime_ns, prior.st_mtime_ns))
        return value
    monkeypatch.setattr(lake_inventory, "_sha256", rewrite_after_hash)
    item = full_lake_inventory_receipt(data_root=tmp_path, source_commit="a" * 40).result["objects"][0]
    assert item["disposition"] == "quarantined"
    assert "file_changed_during_inventory" in item["reason_codes"]


def test_full_inventory_retains_both_layouts_and_all_dispositions(tmp_path):
    for layout in ("canonical/bybit/ETHUSDT", "canonical/perp/binance/SOLUSDT"):
        path = tmp_path / layout / "timeframe=1m/research_panel.parquet"
        path.parent.mkdir(parents=True)
        pd.DataFrame({"ts": pd.date_range("2025-01-01", periods=2, freq="min", tz="UTC"), "close": [1., 2.]}).to_parquet(path)
    broken = tmp_path / "canonical/perp/binance/XUSDT/timeframe=1m/research_panel.parquet"
    broken.parent.mkdir(parents=True)
    broken.write_bytes(b"not parquet")
    link = tmp_path / "raw"
    link.mkdir()
    (link / "outside.parquet").symlink_to(Path("/etc/hostname"))
    receipt = full_lake_inventory_receipt(data_root=tmp_path, source_commit="a" * 40)
    assert verify_receipt(receipt)
    assert receipt.result["object_count"] == 4
    assert receipt.result["dispositions"] == {"cataloged_pending_quality": 2, "quarantined": 2}
    assert all(not item["execution_eligible"] for item in receipt.result["objects"])
    assert {item["instrument"] for item in receipt.result["objects"] if item["disposition"] == "cataloged_pending_quality"} == {"ETHUSDT", "SOLUSDT"}
    assert not any(receipt.authority.values())
    assert full_lake_inventory_receipt(data_root=tmp_path, source_commit="a" * 40).receipt_digest == receipt.receipt_digest


def test_inventory_command_retains_protected_receipt(tmp_path, monkeypatch, capsys):
    root = tmp_path / "lake"
    root.mkdir()
    output = tmp_path / "evidence" / "inventory.json"
    monkeypatch.setattr("sys.argv", ["inventory_full_lake", "--data-root", str(root),
                                    "--source-commit", "a" * 40, "--output", str(output)])
    command = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/inventory_full_lake.py"))
    assert command["main"]() == 0
    assert output.stat().st_mode & 0o777 == 0o600
    assert verify_receipt(json.loads(output.read_text()))
    event = json.loads(capsys.readouterr().out.strip())
    assert event["event"] == "lake_inventory_complete"
    assert event["execution_authority"] is False
