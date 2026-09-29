from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd

from orchestrator.db import ResearchDB


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "disc010_replenisher", ROOT / "scripts" / "replenish_disc010_signal_screens.py"
)
assert SPEC is not None and SPEC.loader is not None
replenisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replenisher)


def create_lake(tmp_path: Path) -> Path:
    root = tmp_path / "research_data"
    records = []
    for index, symbol in enumerate(("BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")):
        path = (
            root
            / "canonical"
            / "perp"
            / "bybit"
            / symbol
            / "timeframe=1m"
            / "research_panel.parquet"
        )
        path.parent.mkdir(parents=True)
        pd.DataFrame(
            {
                "ts": pd.date_range("2025-01-01", periods=2, freq="1min", tz="UTC"),
                "open": [1, 1],
                "high": [1, 1],
                "low": [1, 1],
                "close": [1, 1],
                "volume": [1, 1],
            }
        ).to_parquet(path, index=False)
        records.append(
            {
                "market": "perp",
                "exchange": "bybit",
                "symbol": symbol,
                "dataset": "ohlcv",
                "timeframe": "1m",
                "actual_rows": 800_000 + index,
                "missing_rows": 0,
                "largest_gap_minutes": 1,
                "first_ts": pd.Timestamp("2024-01-01", tz="UTC"),
                "last_ts": pd.Timestamp("2026-01-01", tz="UTC"),
            }
        )
    manifests = root / "manifests"
    manifests.mkdir(parents=True)
    pd.DataFrame(records).to_parquet(manifests / "coverage.parquet", index=False)
    return root


def test_manifest_only_selection_and_frozen_documents(tmp_path):
    data_root = create_lake(tmp_path)
    assets = replenisher.eligible_assets(data_root)

    assert {item["instrument"] for item in assets} == {
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
    }
    basket = replenisher.select_basket(assets, cycle=7, offset=0)
    assert basket == replenisher.select_basket(assets, cycle=7, offset=0)
    state = {"schema_version": "disc010-replenisher-state-v1.0.0", "cycle": 6}
    assignment_path, assignment = replenisher.build_documents(
        basket=basket,
        timeframe="15m",
        cycle=7,
        ordinal=0,
        output_root=tmp_path / "runs",
        data_root=data_root,
        source_commit="a" * 40,
        max_workers=6,
        state=state,
    )

    specification = json.loads(
        Path(assignment["specification"]).read_text(encoding="utf-8")
    )
    bindings = json.loads(Path(assignment["bindings"]).read_text(encoding="utf-8"))
    assert assignment_path.is_file()
    assert assignment["max_workers"] == 6
    assert assignment["data_root"] == str(data_root.resolve())
    assert len(specification["trials"]) == 8
    assert specification["outcome_data_consulted_during_selection"] is False
    assert specification["sealed_oos_start"] == specification["validation_end"]
    assert specification["instruments"] == [item["instrument"] for item in basket]
    assert {item["instrument"] for item in bindings["panels"]} == set(
        specification["instruments"]
    )
    assert len(state["file_digests"]) == 3


def test_basket_selection_skips_assets_without_common_pre_oos_overlap():
    common = [
        {
            "instrument": symbol,
            "first_ts": pd.Timestamp("2024-01-01", tz="UTC"),
            "last_ts": pd.Timestamp("2026-01-01", tz="UTC"),
        }
        for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    ]
    incompatible = {
        "instrument": "XRPUSDT",
        "first_ts": pd.Timestamp("2025-09-01", tz="UTC"),
        "last_ts": pd.Timestamp("2026-01-01", tz="UTC"),
    }

    basket = replenisher.select_basket([*common, incompatible], cycle=20, offset=1)

    assert {item["instrument"] for item in basket} == {
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
    }
    assert replenisher.research_window(basket)["sealed_oos_start"]


def test_active_screen_count_ignores_terminal_and_other_work(tmp_path):
    db = ResearchDB(tmp_path / "capacity.sqlite", repo_root=tmp_path)
    db.init_schema()
    pending = db.enqueue(
        queue_name="approved_backtests",
        item_type="disc010_signal_screen",
        item_id="one",
        payload={},
    )
    terminal = db.enqueue(
        queue_name="approved_backtests",
        item_type="disc010_signal_screen",
        item_id="two",
        payload={},
    )
    db.mark_queue_done(terminal)
    db.enqueue(
        queue_name="approved_backtests",
        item_type="governed_alpha_assignment",
        item_id="three",
        payload={},
    )

    assert pending
    assert replenisher.active_screen_count(db) == 1
    db.close()
