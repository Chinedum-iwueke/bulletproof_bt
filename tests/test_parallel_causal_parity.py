from __future__ import annotations

import json
from pathlib import Path

from bt.experiments import parallel_grid
from bt.logging.run_contract import REQUIRED_ARTIFACTS


def _write_success_run(run_dir: Path, payload: dict[str, object]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "causal_parity.json").write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    (run_dir / "run_status.json").write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    for name in REQUIRED_ARTIFACTS:
        (run_dir / name).write_text("ok", encoding="utf-8")


def _deterministic_causal_payload() -> dict[str, object]:
    # Represents a deterministic no-lookahead path where each step depends only on prior bars.
    prices = [100.0, 101.0, 99.5, 103.0]
    entries = [idx for idx, price in enumerate(prices) if price > prices[max(idx - 1, 0)]]
    exits = [idx for idx in range(1, len(prices)) if prices[idx] < prices[idx - 1]]
    fills = [{"bar": idx, "price": prices[idx]} for idx in entries]
    trades = [{"entry": e, "exit": exits[0] if exits else len(prices) - 1} for e in entries]
    equity = [100000.0 + (price - prices[0]) * 10 for price in prices]
    return {
        "entries": entries,
        "exits": exits,
        "fills": fills,
        "trades": trades,
        "equity": equity,
    }


def test_parallel_runner_causal_parity_with_serial_stub(monkeypatch, tmp_path: Path) -> None:
    manifest_row = {
        "row_id": "row_00001",
        "hypothesis_id": "L1-H1",
        "hypothesis_path": "research/hypotheses/l1_h1_vol_floor_trend.yaml",
        "phase": "tier2",
        "tier": "Tier2",
        "variant_id": "g00000",
        "config_hash": "abc",
        "params_json": "{}",
        "run_slug": "g00000__tier2",
        "output_dir": "runs/row_00001__g00000__tier2",
        "expected_status": "pending",
        "enabled": "true",
        "notes": "",
    }

    serial_payload = _deterministic_causal_payload()

    class DummyFuture:
        def result(self):
            return 0, ""

    class DummyExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def submit(self, fn, **kwargs):
            fn(**kwargs)
            return DummyFuture()

    def fake_execute_manifest_row(**kwargs):
        row = kwargs["row"]
        run_dir = Path(kwargs["experiment_root"]) / row["output_dir"]
        _write_success_run(run_dir, serial_payload)
        return 0, ""

    monkeypatch.setattr(parallel_grid, "ProcessPoolExecutor", DummyExecutor)
    monkeypatch.setattr(parallel_grid, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(parallel_grid, "_execute_manifest_row", fake_execute_manifest_row)

    config = tmp_path / "engine.yaml"
    config.write_text("{}", encoding="utf-8")
    data = tmp_path / "dataset"
    data.mkdir()

    statuses, failures = parallel_grid.run_hypothesis_manifest_in_parallel(
        manifest_rows=[manifest_row],
        experiment_root=tmp_path / "out",
        config_path=config,
        local_config=None,
        data_path=data,
        max_workers=1,
        skip_completed=False,
        override_paths=[],
        dry_run=False,
    )

    assert failures == []
    assert statuses[0]["status"] == "COMPLETED"

    run_dir = tmp_path / "out" / manifest_row["output_dir"]
    parallel_payload = json.loads((run_dir / "causal_parity.json").read_text(encoding="utf-8"))
    assert parallel_payload == serial_payload
