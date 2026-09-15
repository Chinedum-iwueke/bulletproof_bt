import pytest

from bt.institutional import lake_inventory
from bt.institutional.receipt import digest, verify_receipt


def objects(count):
    for index in range(count):
        yield {"partition_id": f"canonical/perp/bybit/ETHUSDT/{index:08d}.parquet",
               "market": "perp", "venue": "bybit", "instrument": "ETHUSDT",
               "execution_eligible": False, "disposition": "cataloged_pending_quality"}


def test_complete_root_binds_every_bounded_shard(monkeypatch, tmp_path):
    monkeypatch.setattr(lake_inventory, "iter_lake_inventory", lambda **kwargs: objects(23))
    shards = []
    receipt = lake_inventory.sharded_lake_inventory_receipt(
        data_root=tmp_path, source_commit="a" * 40, run_id="run-one",
        emit_shard=shards.append, shard_size=5)
    assert verify_receipt(receipt)
    assert [item.result["object_count"] for item in shards] == [5, 5, 5, 5, 3]
    assert receipt.result["object_count"] == 23
    assert receipt.result["shard_count"] == 5
    assert receipt.result["assets"] == [("perp", "bybit", "ETHUSDT")]
    assert receipt.dataset_digest == digest(receipt.result["shards"])
    assert [item.receipt_digest for item in shards] == [item["receipt_digest"] for item in receipt.result["shards"]]
    assert all(verify_receipt(item) for item in shards)
    assert all(not any(item.authority.values()) for item in shards)


def test_shard_failure_never_emits_complete_receipt(monkeypatch, tmp_path):
    monkeypatch.setattr(lake_inventory, "iter_lake_inventory", lambda **kwargs: objects(23))

    def failed(receipt):
        raise RuntimeError("shard custody unavailable")

    with pytest.raises(RuntimeError, match="custody"):
        lake_inventory.sharded_lake_inventory_receipt(
            data_root=tmp_path, source_commit="a" * 40, run_id="run-one",
            emit_shard=failed, shard_size=5)


def test_unbounded_shard_size_rejected(tmp_path):
    with pytest.raises(ValueError, match="bounded"):
        lake_inventory.sharded_lake_inventory_receipt(
            data_root=tmp_path, source_commit="a" * 40, run_id="run-one",
            emit_shard=lambda receipt: None, shard_size=250001)


def test_actual_walk_has_disjoint_globally_ordered_shards(tmp_path):
    for relative in ("raw/binance/ETHUSDT/ohlcv/timeframe=1m/data.txt",
                     "raw/binance/ETHUSDT/ohlcv/timeframe=1m/chunks/part.txt",
                     "canonical/bybit/ETHUSDT/timeframe=1m/research_panel.txt",
                     "manifests/coverage.txt"):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not parquet; retained with explicit adapter requirement")
    shards = []
    receipt = lake_inventory.sharded_lake_inventory_receipt(
        data_root=tmp_path, source_commit="a" * 40, run_id="run-one",
        emit_shard=shards.append, shard_size=2)
    paths = [item["partition_id"] for shard in shards for item in shard.result["objects"]]
    assert paths == sorted(paths)
    assert receipt.result["object_count"] == 4
    assert receipt.result["dispositions"] == {"quarantined": 4}
