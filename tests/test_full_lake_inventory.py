from pathlib import Path
import json
import runpy

import pandas as pd

from bt.institutional.lake_inventory import full_lake_inventory_receipt
from bt.institutional.receipt import verify_receipt


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
