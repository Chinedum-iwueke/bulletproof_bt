from __future__ import annotations

import json
from pathlib import Path

from bt.experiments import parallel_grid
from bt.experiments.precompute_cache import stable_cache_key
from bt.experiments.wave_scheduler import iter_waves, resolve_wave_size
from bt.experiments.worker_bootstrap import apply_thread_caps


def _row() -> dict[str, str]:
    return {
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


def test_spawn_pool_context_is_used(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class DummyFuture:
        def result(self) -> tuple[int, str]:
            return 0, ""

    class DummyExecutor:
        def __init__(self, *args, **kwargs):
            captured["mp_context"] = kwargs.get("mp_context")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def submit(self, *args, **kwargs):
            return DummyFuture()

    monkeypatch.setattr(parallel_grid, "ProcessPoolExecutor", DummyExecutor)
    monkeypatch.setattr(parallel_grid, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(parallel_grid, "_execute_manifest_row", lambda **kwargs: (0, ""))

    config = tmp_path / "engine.yaml"
    config.write_text("{}", encoding="utf-8")
    data = tmp_path / "dataset"
    data.mkdir()

    statuses, _ = parallel_grid.run_hypothesis_manifest_in_parallel(
        manifest_rows=[_row()],
        experiment_root=tmp_path / "out",
        config_path=config,
        local_config=None,
        data_path=data,
        max_workers=1,
        skip_completed=False,
        override_paths=[],
        dry_run=False,
    )

    assert statuses[0]["status"] in {"COMPLETED", "FAILED"}
    assert captured["mp_context"].get_start_method() == "spawn"


def test_thread_caps_respect_explicit_environment(monkeypatch) -> None:
    monkeypatch.setenv("OMP_NUM_THREADS", "8")
    result = apply_thread_caps()
    assert result.effective_thread_caps["OMP_NUM_THREADS"] == "8"
    assert "OMP_NUM_THREADS" not in result.changed_thread_caps
    assert all(value for value in result.effective_thread_caps.values())


def test_wave_scheduler_chunks_deterministically() -> None:
    items = list(range(7))
    waves = list(iter_waves(items, wave_size=3))
    assert waves == [[0, 1, 2], [3, 4, 5], [6]]
    assert resolve_wave_size(max_workers=4) == 8


def test_precompute_cache_key_is_deterministic() -> None:
    key_a = stable_cache_key(
        dataset_id="d1",
        timeframe="15m",
        family="ema",
        params={"fast": 9, "slow": 21},
        engine_version="v2",
    )
    key_b = stable_cache_key(
        dataset_id="d1",
        timeframe="15m",
        family="ema",
        params={"slow": 21, "fast": 9},
        engine_version="v2",
    )
    key_c = stable_cache_key(
        dataset_id="d2",
        timeframe="15m",
        family="ema",
        params={"fast": 9, "slow": 21},
        engine_version="v2",
    )
    assert key_a == key_b
    assert key_a != key_c


def test_worker_failure_artifacts_are_written(monkeypatch, tmp_path: Path) -> None:
    row = _row()
    experiment_root = tmp_path / "exp"

    monkeypatch.setattr(parallel_grid, "execute_hypothesis_variant", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    code, message = parallel_grid._execute_manifest_row(
        row=row,
        config_path="configs/engine.yaml",
        local_config=None,
        data_path=str(tmp_path),
        experiment_root=str(experiment_root),
        override_paths=[],
        shared_dataset_plan={"dataset_id": "d1", "source": "attached_from_cache"},
        precompute_registry={},
    )

    run_dir = experiment_root / row["output_dir"]
    assert code == 1
    assert "boom" in message
    assert (run_dir / "worker.log").exists()
    assert (run_dir / "worker_exception.txt").exists()
    assert (run_dir / "faulthandler.log").exists()
    context = json.loads((run_dir / "run_context.json").read_text(encoding="utf-8"))
    assert context["row_id"] == row["row_id"]
