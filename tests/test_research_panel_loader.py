from __future__ import annotations

import pandas as pd
import pytest

from bt.core.errors import DataError
from bt.data.load_feed import load_feed
from bt.data.research_panel_loader import (
    apply_volatile_membership,
    build_streaming_research_panel_feed_from_config,
    load_research_panels,
    load_stable_research_panel,
    load_volatile_research_panel,
)
from bt.research_data.jobs.materialize import materialize_volatile_panel
from bt.research_data.storage import ResearchDataStore


def _panel(root, symbol: str, closes: list[float]) -> pd.DataFrame:
    ts = pd.date_range(pd.Timestamp("2021-01-01 00:00", tz="UTC"), periods=len(closes), freq="1min")
    df = pd.DataFrame(
        {
            "ts": ts,
            "exchange": "binance",
            "symbol": symbol,
            "canonical_symbol": [symbol.removesuffix("USDT") + "-USDT-PERP"] * len(closes),
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": [1.0] * len(closes),
            "quote_volume": closes,
            "mark_close": closes,
            "index_close": closes,
            "funding_rate": [0.01] * len(closes),
            "funding_source_ts": ts,
            "open_interest": [100.0] * len(closes),
            "oi_source_ts": ts,
            "oi_change_1": [0.0] * len(closes),
            "oi_change_pct_1": [0.0] * len(closes),
            "premium_mark_vs_index": [0.0] * len(closes),
            "basis_close_vs_index": [0.0] * len(closes),
            "liq_buy_notional": [10.0] * len(closes),
            "liq_sell_notional": [5.0] * len(closes),
        }
    )
    path = root / "canonical" / "binance" / symbol / "timeframe=1m" / "research_panel.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return df


def test_stable_loader_loads_fixed_symbols(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0])
    _panel(root, "ETHUSDT", [3.0, 4.0])
    manifest = pd.DataFrame(
        {
            "exchange": ["binance", "binance"],
            "native_symbol": ["BTCUSDT", "ETHUSDT"],
            "available": [True, True],
        }
    )
    manifest_path = root / "manifests" / "stable_universe.parquet"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_parquet(manifest_path, index=False)

    loaded = load_stable_research_panel(root, "binance", "1m")

    assert set(loaded["symbol"]) == {"BTCUSDT", "ETHUSDT"}
    assert "mark_close" in loaded.columns


def test_research_panel_config_applies_date_and_symbol_scope(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0])
    _panel(root, "ETHUSDT", [4.0, 5.0, 6.0])
    manifest = pd.DataFrame(
        {
            "exchange": ["binance", "binance"],
            "native_symbol": ["BTCUSDT", "ETHUSDT"],
            "available": [True, True],
        }
    )
    manifest_path = root / "manifests" / "stable_universe.parquet"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_parquet(manifest_path, index=False)

    feed = load_feed(
        str(root),
        {
            "data": {
                "dataset_kind": "research_panel",
                "exchange": "binance",
                "universe": "stable",
                "stable_manifest": str(manifest_path),
                "root": str(root),
                "timeframe": "1m",
                "symbols_subset": ["ETHUSDT"],
                "date_range": {"start": "2021-01-01T00:01:00Z", "end": "2021-01-01T00:03:00Z"},
            }
        },
    )

    first = feed.next()
    second = feed.next()
    third = feed.next()

    assert first is not None
    assert [bar.symbol for bar in first] == ["ETHUSDT"]
    assert first[0].ts == pd.Timestamp("2021-01-01 00:01", tz="UTC")
    assert second is not None
    assert second[0].ts == pd.Timestamp("2021-01-01 00:02", tz="UTC")
    assert third is None


def test_volatile_loader_changes_active_universe_by_timestamp(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0])
    _panel(root, "ETHUSDT", [4.0, 5.0, 6.0])
    membership = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 00:01", tz="UTC"),
            ],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
        }
    )
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)

    loaded = load_volatile_research_panel(root, "binance", membership_path, "1m")

    active = loaded[loaded["volatile_active"].astype(bool)]
    assert active.loc[active["ts"].eq(pd.Timestamp("2021-01-01 00:00", tz="UTC")), "symbol"].tolist() == ["BTCUSDT"]
    assert active.loc[active["ts"].eq(pd.Timestamp("2021-01-01 00:01", tz="UTC")), "symbol"].tolist() == ["ETHUSDT"]


def test_materialized_volatile_panel_contains_only_active_intervals(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0, 4.0])
    _panel(root, "ETHUSDT", [5.0, 6.0, 7.0, 8.0])
    membership = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 00:02", tz="UTC"),
            ],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "universe": ["volatile_data_1m_canonical", "volatile_data_1m_canonical"],
        }
    )
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)

    path = materialize_volatile_panel(
        "binance",
        "1m",
        membership_path=membership_path,
        start="2021-01-01T00:00:00Z",
        end="2021-01-01T00:04:00Z",
        store=ResearchDataStore(root),
        row_group_size=2,
    )

    materialized = pd.read_parquet(path)
    assert materialized[["ts", "symbol"]].to_dict("records") == [
        {"ts": pd.Timestamp("2021-01-01 00:00", tz="UTC"), "symbol": "BTCUSDT"},
        {"ts": pd.Timestamp("2021-01-01 00:01", tz="UTC"), "symbol": "BTCUSDT"},
        {"ts": pd.Timestamp("2021-01-01 00:02", tz="UTC"), "symbol": "ETHUSDT"},
        {"ts": pd.Timestamp("2021-01-01 00:03", tz="UTC"), "symbol": "ETHUSDT"},
    ]
    assert materialized["volatile_active"].all()
    assert "funding_rate" in materialized.columns


def test_volatile_streaming_feed_prefers_materialized_panel(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0])
    membership = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2021-01-01 00:00", tz="UTC")],
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
            "universe": ["volatile_data_1m_canonical"],
        }
    )
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)
    materialize_volatile_panel(
        "binance",
        "1m",
        membership_path=membership_path,
        start="2021-01-01T00:00:00Z",
        end="2021-01-01T00:02:00Z",
        store=ResearchDataStore(root),
    )

    feed = build_streaming_research_panel_feed_from_config(
        {
            "dataset_kind": "research_panel",
            "exchange": "binance",
            "universe": "volatile",
            "membership_path": str(membership_path),
            "root": str(root),
            "timeframe": "1m",
        }
    )

    assert type(feed).__name__ == "MaterializedResearchPanelStreamingFeed"
    first = feed.next()
    assert first is not None
    assert [bar.symbol for bar in first] == ["BTCUSDT"]
    assert first[0].extra["universe_active"] is True


def test_materialized_volatile_feed_keeps_required_inactive_symbols(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0])
    _panel(root, "ETHUSDT", [4.0, 5.0, 6.0])
    membership = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 00:01", tz="UTC"),
            ],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
            "universe": ["volatile_data_1m_canonical", "volatile_data_1m_canonical"],
        }
    )
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)
    materialize_volatile_panel(
        "binance",
        "1m",
        membership_path=membership_path,
        start="2021-01-01T00:00:00Z",
        end="2021-01-01T00:03:00Z",
        store=ResearchDataStore(root),
    )
    feed = build_streaming_research_panel_feed_from_config(
        {
            "dataset_kind": "research_panel",
            "exchange": "binance",
            "universe": "volatile",
            "membership_path": str(membership_path),
            "root": str(root),
            "timeframe": "1m",
        }
    )

    first = feed.next()
    assert first is not None
    assert [bar.symbol for bar in first] == ["BTCUSDT"]
    feed.set_required_symbols(["BTCUSDT"])

    second = feed.next()
    assert second is not None
    assert [bar.symbol for bar in second] == ["BTCUSDT", "ETHUSDT"]
    active_by_symbol = {bar.symbol: bar.extra["volatile_active"] for bar in second}
    assert active_by_symbol == {"BTCUSDT": False, "ETHUSDT": True}


def test_strategy_cannot_access_inactive_symbols(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0])
    _panel(root, "ETHUSDT", [3.0, 4.0])
    membership = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2021-01-01 00:00", tz="UTC")],
            "exchange": ["binance"],
            "symbol": ["BTCUSDT"],
        }
    )
    panels = load_research_panels(root, "binance", ["BTCUSDT", "ETHUSDT"], "1m")
    active = apply_volatile_membership(panels, membership)
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)
    feed = load_feed(
        str(root),
        {
            "data": {
                "dataset_kind": "research_panel",
                "exchange": "binance",
                "universe": "volatile",
                "membership_path": str(root / "manifests" / "volatile_universe_membership.parquet"),
                "root": str(root),
                "timeframe": "1m",
            }
        },
    )
    first = feed.next()

    assert set(active["symbol"]) == {"BTCUSDT"}
    assert active.loc[active["symbol"].eq("BTCUSDT"), "volatile_active"].all()
    assert first is not None
    assert [bar.symbol for bar in first] == ["BTCUSDT"]
    assert "funding_rate" in first[0].extra
    assert "liq_buy_notional" in first[0].extra


def test_volatile_streaming_feed_emits_only_active_symbols_and_required_positions(tmp_path) -> None:
    root = tmp_path / "research_data"
    _panel(root, "BTCUSDT", [1.0, 2.0, 3.0])
    _panel(root, "ETHUSDT", [3.0, 4.0, 5.0])
    membership = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2021-01-01 00:00", tz="UTC"),
                pd.Timestamp("2021-01-01 00:01", tz="UTC"),
            ],
            "exchange": ["binance", "binance"],
            "symbol": ["BTCUSDT", "ETHUSDT"],
        }
    )
    membership_path = root / "manifests" / "volatile_universe_membership.parquet"
    membership_path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_parquet(membership_path, index=False)

    feed = build_streaming_research_panel_feed_from_config(
        {
            "dataset_kind": "research_panel",
            "exchange": "binance",
            "universe": "volatile",
            "membership_path": str(membership_path),
            "root": str(root),
            "timeframe": "1m",
        }
    )

    first = feed.next()
    assert first is not None
    assert [bar.symbol for bar in first] == ["BTCUSDT"]
    assert first[0].extra["volatile_active"] is True

    feed.set_required_symbols(["BTCUSDT"])
    second = feed.next()
    assert second is not None
    assert [bar.symbol for bar in second] == ["BTCUSDT", "ETHUSDT"]
    active_by_symbol = {bar.symbol: bar.extra["volatile_active"] for bar in second}
    assert active_by_symbol == {"BTCUSDT": False, "ETHUSDT": True}

    feed.set_required_symbols([])
    third = feed.next()
    assert third is not None
    assert [bar.symbol for bar in third] == ["ETHUSDT"]


def test_funding_and_oi_source_ts_never_exceed_bar_ts(tmp_path) -> None:
    root = tmp_path / "research_data"
    df = _panel(root, "BTCUSDT", [1.0])
    df.loc[0, "funding_source_ts"] = pd.Timestamp("2021-01-01 00:01", tz="UTC")
    path = root / "canonical" / "binance" / "BTCUSDT" / "timeframe=1m" / "research_panel.parquet"
    df.to_parquet(path, index=False)

    with pytest.raises(DataError, match="funding_source_ts"):
        load_research_panels(root, "binance", ["BTCUSDT"], "1m")


def test_research_panel_loader_preserves_precomputed_state_columns(tmp_path) -> None:
    root = tmp_path / "research_data"
    df = _panel(root, "BTCUSDT", [1.0])
    df["entry_state_csi_raw"] = [0.7]
    df["entry_state_csi_source"] = ["enriched"]
    path = root / "canonical" / "binance" / "BTCUSDT" / "timeframe=1m" / "research_panel.parquet"
    df.to_parquet(path, index=False)

    feed = build_streaming_research_panel_feed_from_config(
        {
            "dataset_kind": "research_panel",
            "exchange": "binance",
            "universe": "custom",
            "symbols": ["BTCUSDT"],
            "root": str(root),
            "timeframe": "1m",
        }
    )
    bars = feed.next()

    assert bars is not None
    assert bars[0].extra["entry_state_csi_raw"] == 0.7
    assert bars[0].extra["entry_state_csi_source"] == "enriched"


def test_missing_panels_produce_clear_errors(tmp_path) -> None:
    with pytest.raises(DataError, match="Missing research panel"):
        load_research_panels(tmp_path / "research_data", "binance", ["BTCUSDT"], "1m")
