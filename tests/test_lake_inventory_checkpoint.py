import pandas as pd
import pytest

from bt.institutional import lake_inventory
from bt.institutional.lake_inventory_checkpoint import InventoryCheckpoint


def panel(root, asset="ETHUSDT", rows=2):
    path = root / f"canonical/perp/bybit/{asset}/timeframe=1m/research_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"ts": pd.date_range("2025-01-01", periods=rows, freq="min", tz="UTC")}).to_parquet(path)
    return path


def test_interrupted_object_traversal_recovers_without_rehash(tmp_path, monkeypatch):
    root = tmp_path / "lake"
    panel(root)
    cache = InventoryCheckpoint(tmp_path / "state/cache.sqlite", root=root, source_commit="a" * 40)
    iterator = lake_inventory.iter_lake_inventory(data_root=root, checkpoint=cache)
    first = next(iterator)
    iterator.close()
    cache.close()
    cache = InventoryCheckpoint(tmp_path / "state/cache.sqlite", root=root, source_commit="a" * 40)
    def unexpected(handle):
        raise AssertionError("unchanged object should not be rehashed")
    monkeypatch.setattr(lake_inventory, "_sha256", unexpected)
    assert list(lake_inventory.iter_lake_inventory(data_root=root, checkpoint=cache)) == [first]
    assert cache.reused == 1
    cache.close()


def test_changes_additions_and_deletions_are_traversed(tmp_path):
    root = tmp_path / "lake"
    old = panel(root)
    cache = InventoryCheckpoint(tmp_path / "cache.sqlite", root=root, source_commit="a" * 40)
    list(lake_inventory.iter_lake_inventory(data_root=root, checkpoint=cache))
    panel(root, rows=3)
    panel(root, "SOLUSDT")
    items = list(lake_inventory.iter_lake_inventory(data_root=root, checkpoint=cache))
    assert len(items) == 2 and cache.reused == 0
    assert next(item for item in items if item["instrument"] == "ETHUSDT")["row_count"] == 3
    old.unlink()
    items = list(lake_inventory.iter_lake_inventory(data_root=root, checkpoint=cache))
    assert [item["instrument"] for item in items] == ["SOLUSDT"]
    cache.close()


def test_source_binding_invalidates_cache(tmp_path):
    root = tmp_path / "lake"
    panel(root)
    path = tmp_path / "cache.sqlite"
    for source in ("a" * 40, "b" * 40):
        cache = InventoryCheckpoint(path, root=root, source_commit=source)
        list(lake_inventory.iter_lake_inventory(data_root=root, checkpoint=cache))
        assert cache.reused == 0 and cache.written == 1
        cache.close()


def test_checkpoint_rejects_public_or_symlink_files(tmp_path):
    root = tmp_path / "lake"
    panel(root)
    path = tmp_path / "cache.sqlite"
    path.touch(mode=0o644)
    with pytest.raises(ValueError, match="private"):
        InventoryCheckpoint(path, root=root, source_commit="a" * 40)
    path.unlink()
    path.symlink_to(tmp_path / "target")
    with pytest.raises(OSError):
        InventoryCheckpoint(path, root=root, source_commit="a" * 40)
