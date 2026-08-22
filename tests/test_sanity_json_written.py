from __future__ import annotations

import json
from pathlib import Path

import pytest

from bt import api
from bt.api import run_backtest


def test_sanity_json_written_for_successful_run(tmp_path: Path) -> None:
    run_dir = Path(
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name="sanity-success",
        )
    )

    sanity_path = run_dir / "sanity.json"
    assert sanity_path.exists()
    payload = json.loads(sanity_path.read_text(encoding="utf-8"))

    int_fields = [
        "signals_emitted",
        "signals_approved",
        "signals_rejected",
        "fills",
        "closed_trades",
        "forced_liquidations",
    ]
    for field in int_fields:
        assert isinstance(payload[field], int)

    assert payload["signals_emitted"] == payload["signals_approved"] + payload["signals_rejected"]
    assert payload["fills"] >= payload["closed_trades"]

    if payload["signals_rejected"] > 0:
        assert payload["rejected_by_reason"]


def test_sanity_json_written_when_run_fails_after_run_dir_created(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original_build_engine = api._build_engine

    def _build_engine_that_crashes(config: dict, datafeed: object, run_dir: Path, sanity_counters=None, **kwargs):
        engine = original_build_engine(config, datafeed, run_dir, sanity_counters=sanity_counters, **kwargs)

        def _boom() -> None:
            raise RuntimeError("forced test failure")

        engine.run = _boom  # type: ignore[method-assign]
        return engine

    monkeypatch.setattr(api, "_build_engine", _build_engine_that_crashes)

    run_name = "sanity-failure"
    with pytest.raises(RuntimeError, match="forced test failure"):
        run_backtest(
            config_path="configs/engine.yaml",
            data_path="data/curated/sample.csv",
            out_dir=str(tmp_path / "out"),
            run_name=run_name,
        )

    run_dir = tmp_path / "out" / run_name
    assert (run_dir / "sanity.json").exists()
