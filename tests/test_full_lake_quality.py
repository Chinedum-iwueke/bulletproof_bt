from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bt.institutional.lake_inventory import full_lake_inventory_receipt, sharded_lake_inventory_receipt
from bt.institutional.lake_quality import full_lake_quality_receipt
from bt.institutional.receipt import verify_receipt

COMMIT = "a" * 40
START = datetime(2025, 1, 1, tzinfo=UTC)


def panel(root, *, offsets=(0, 1, 2), venue="bybit", prices=None):
    path = root / "canonical" / "perp" / "bybit" / "ETHUSDT" / "timeframe=1m" / "research_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    count = len(offsets)
    pq.write_table(pa.table({
        "ts": pa.array([START + timedelta(minutes=value) for value in offsets], type=pa.timestamp("us", tz="UTC")),
        "exchange": [venue] * count, "symbol": ["ETHUSDT"] * count,
        "canonical_symbol": ["ETH-USDT-PERP"] * count,
        "open": prices or [10.] * count, "high": [11.] * count,
        "low": [9.] * count, "close": [10.] * count, "volume": [100.] * count,
    }), path)
    return path


def quality(root, inventory=None):
    inventory = inventory or full_lake_inventory_receipt(data_root=root, source_commit=COMMIT)
    return full_lake_quality_receipt(data_root=root, inventory=inventory,
                                    window_start=START.isoformat(),
                                    window_end=(START + timedelta(minutes=3)).isoformat(),
                                    source_commit=COMMIT)


def test_non_btc_panel_quality_does_not_grant_execution(tmp_path):
    panel(tmp_path)
    receipt = quality(tmp_path)
    assert verify_receipt(receipt)
    item = receipt.result["objects"][0]
    assert item["instrument"] == "ETHUSDT"
    assert item["window_rows"] == 3
    assert item["panel_quality_passed"]
    assert not item["execution_eligible"]
    assert not any(receipt.authority.values())


@pytest.mark.parametrize("offsets,reason", [((0, 2), "complete_window_grid"),
                                             ((0, 1, 1), "strict_timestamp_order"),
                                             ((0, 2, 1), "strict_timestamp_order")])
def test_measured_gaps_duplicates_and_disorder(tmp_path, offsets, reason):
    panel(tmp_path, offsets=offsets)
    item = quality(tmp_path).result["objects"][0]
    assert reason in item["reason_codes"]
    assert not item["panel_quality_passed"]


def test_content_change_cannot_reuse_inventory(tmp_path):
    panel(tmp_path)
    inventory = full_lake_inventory_receipt(data_root=tmp_path, source_commit=COMMIT)
    panel(tmp_path, venue="binance")
    item = quality(tmp_path, inventory).result["objects"][0]
    assert "inventory_content_changed" in item["reason_codes"]


def test_price_and_identity_checks_are_not_footer_inferences(tmp_path):
    panel(tmp_path, venue="binance", prices=[10., float("nan"), -1.])
    item = quality(tmp_path).result["objects"][0]
    assert "valid_ohlcv" in item["reason_codes"]
    assert "path_exchange_symbol_consistent" in item["reason_codes"]
    assert item["invalid_ohlcv_rows"] == 2


def test_non_panel_objects_remain_accounted_with_adapter_gap(tmp_path):
    path = panel(tmp_path)
    path.rename(path.with_name("ohlcv.parquet"))
    result = quality(tmp_path).result
    assert result["objects"] == []
    assert result["inventoried_object_count"] == 1
    assert result["unprocessed_dispositions"] == {"non_panel_adapter_required": 1}


def test_sharded_complete_inventory_is_consumed_without_recombining_objects(tmp_path):
    path = panel(tmp_path)
    path.with_name("ohlcv.parquet").write_bytes(path.read_bytes())
    shards = {}
    inventory = sharded_lake_inventory_receipt(
        data_root=tmp_path, source_commit=COMMIT, run_id="run-one", shard_size=1,
        emit_shard=lambda shard: shards.update({shard.receipt_digest: shard}))
    receipt = full_lake_quality_receipt(
        data_root=tmp_path, inventory=inventory, window_start=START.isoformat(),
        window_end=(START + timedelta(minutes=3)).isoformat(), source_commit=COMMIT,
        load_shard=lambda descriptor: shards[descriptor["receipt_digest"]])
    assert receipt.result["inventoried_object_count"] == 2
    assert len(receipt.result["objects"]) == 1
    assert receipt.result["objects"][0]["panel_quality_passed"]
    assert receipt.result["unprocessed_dispositions"] == {"non_panel_adapter_required": 1}
    assert verify_receipt(receipt)


def test_inventory_to_quality_cli_handoff(tmp_path):
    root = tmp_path / "lake"
    panel(root)
    scripts = Path(__file__).parents[1] / "scripts"
    inventory = tmp_path / "evidence" / "inventory.json"
    result = subprocess.run([sys.executable, str(scripts / "inventory_full_lake.py"),
                             "--data-root", str(root), "--source-commit", COMMIT,
                             "--output", str(inventory), "--run-id", "cli-test"],
                            capture_output=True, text=True, check=True)
    assert "lake_inventory_shard_ready" in result.stdout
    output = inventory.with_name("quality.json")
    subprocess.run([sys.executable, str(scripts / "quality_full_lake.py"),
                    "--data-root", str(root), "--inventory", str(inventory),
                    "--source-commit", COMMIT, "--output", str(output),
                    "--window-start", START.isoformat(),
                    "--window-end", (START + timedelta(minutes=3)).isoformat()],
                   capture_output=True, text=True, check=True)
    document = json.loads(output.read_text())
    assert verify_receipt(document)
    assert document["result"]["objects"][0]["panel_quality_passed"]
    assert os.stat(output).st_mode & 0o777 == 0o600
