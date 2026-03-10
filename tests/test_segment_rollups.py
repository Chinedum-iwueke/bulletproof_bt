from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from bt.analytics.segment_rollups import (
    build_run_segment_rollups,
    compute_segment_rollups,
    load_trades_with_entry_metadata,
)
from bt.experiments.parallel_grid import _materialize_phase_segment_rollups


def _write_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    trades = pd.DataFrame(
        [
            {
                "entry_ts": "2024-01-01T00:00:00+00:00",
                "exit_ts": "2024-01-01T00:15:00+00:00",
                "symbol": "BTCUSD",
                "side": "BUY",
                "pnl_net": 10,
                "r_multiple_net": 1.0,
                "hold_bars": 3,
            },
            {
                "entry_ts": "2024-01-01T00:30:00+00:00",
                "exit_ts": "2024-01-01T00:45:00+00:00",
                "symbol": "BTCUSD",
                "side": "SELL",
                "pnl_net": -5,
                "r_multiple_net": -0.5,
                "hold_bars": 2,
            },
            {
                "entry_ts": "2024-01-01T01:00:00+00:00",
                "exit_ts": "2024-01-01T01:15:00+00:00",
                "symbol": "BTCUSD",
                "side": "BUY",
                "pnl_net": 20,
                "r_multiple_net": 2.0,
                "hold_bars": 5,
            },
        ]
    )
    trades.to_csv(run_dir / "trades.csv", index=False)

    fills = [
        {
            "ts": "2024-01-01T00:00:00+00:00",
            "symbol": "BTCUSD",
            "side": "BUY",
            "qty": 1,
            "price": 100,
            "metadata": {"entry_reason": "compression_fade_long", "q_comp": 0.2, "comp_gate_t": True},
        },
        {
            "ts": "2024-01-01T00:30:00+00:00",
            "symbol": "BTCUSD",
            "side": "SELL",
            "qty": 1,
            "price": 101,
            "metadata": {"entry_reason": "compression_fade_short", "q_comp": 0.2, "comp_gate_t": True},
        },
        {
            "ts": "2024-01-01T01:00:00+00:00",
            "symbol": "BTCUSD",
            "side": "BUY",
            "qty": 1,
            "price": 99,
            "metadata": {"entry_reason": "compression_fade_long", "q_comp": None, "comp_gate_t": True},
        },
    ]
    with (run_dir / "fills.jsonl").open("w", encoding="utf-8") as h:
        for rec in fills:
            h.write(json.dumps(rec) + "\n")


def test_metadata_extraction_and_grouping(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_run(run_dir)

    enriched = load_trades_with_entry_metadata(run_dir, required_segment_keys=["entry_reason", "q_comp"])
    assert "entry_meta__entry_reason" in enriched.columns

    rows = compute_segment_rollups(enriched, segment_keys=["entry_reason"])
    assert [row["segment_value_json"] for row in rows] == sorted([row["segment_value_json"] for row in rows])
    long_row = next(r for r in rows if "compression_fade_long" in r["segment_value_json"])
    assert long_row["n_trades"] == 2
    assert long_row["ev_r_net"] == pytest.approx(1.5)


def test_missing_key_and_nulls(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_run(run_dir)

    with pytest.raises(ValueError, match="Missing requested segment keys"):
        load_trades_with_entry_metadata(run_dir, required_segment_keys=["rvhat_pct_t"])

    enriched = load_trades_with_entry_metadata(run_dir, required_segment_keys=["q_comp"])
    rows = compute_segment_rollups(enriched, segment_keys=["q_comp"])
    assert any("__MISSING__" in row["segment_value_json"] for row in rows)


def test_writes_artifacts_and_l1_h2_defaults(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    _write_run(run_dir)
    (run_dir / "config_used.yaml").write_text("strategy:\n  name: l1_h2_compression_mean_reversion\n", encoding="utf-8")

    rows = build_run_segment_rollups(run_dir, hypothesis_id="L1-H2")
    assert rows
    assert (run_dir / "segment_rollups.csv").exists()
    assert (run_dir / "segment_rollups.jsonl").exists()


def test_phase_level_aggregation(tmp_path: Path) -> None:
    exp = tmp_path / "exp"
    run_dir = exp / "runs/row_00001__g0__tier2"
    _write_run(run_dir)
    build_run_segment_rollups(run_dir, segment_keys=["entry_reason"], hypothesis_id="L1-H2")

    manifest_rows = [
        {
            "row_id": "row_00001",
            "variant_id": "g0",
            "tier": "Tier2",
            "hypothesis_id": "L1-H2",
            "output_dir": "runs/row_00001__g0__tier2",
        }
    ]
    status_rows = [{"row_id": "row_00001", "status": "COMPLETED"}]
    _materialize_phase_segment_rollups(exp, manifest_rows, status_rows)

    out_path = exp / "summaries" / "phase_segment_rollups.csv"
    with out_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) >= 1
    assert rows[0]["row_id"] == "row_00001"
