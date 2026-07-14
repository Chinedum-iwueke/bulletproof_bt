from __future__ import annotations

from bt.experiments.resource_controls import MemorySnapshot
from orchestrator.db import ResearchDB
from orchestrator.global_capacity_scheduler import (
    CapacitySchedulerConfig,
    ManagedJob,
    active_worker_slots,
    estimate_worker_slots,
    paused_worker_slots,
    should_pause_for_memory,
    should_resume_for_memory,
)


def test_estimate_worker_slots_counts_parallel_dataset_worker_budget_once() -> None:
    cfg = CapacitySchedulerConfig(max_workers_per_job=20)
    daemon_cfg = {"default_max_workers": 6, "volatile_max_workers": 5, "runner_parallel_datasets": False}
    payload = {"max_workers": 8, "volatile_max_workers": 4, "parallel_datasets": True}

    assert estimate_worker_slots(payload, cfg, daemon_cfg) == 8


def test_estimate_worker_slots_is_capped_per_job() -> None:
    cfg = CapacitySchedulerConfig(max_workers_per_job=10)
    daemon_cfg = {"default_max_workers": 12, "volatile_max_workers": 12}
    payload = {"max_workers": 12, "volatile_max_workers": 12, "parallel_datasets": True}

    assert estimate_worker_slots(payload, cfg, daemon_cfg) == 10


def test_memory_pause_resume_thresholds_have_hysteresis() -> None:
    cfg = CapacitySchedulerConfig(pause_free_ram_gb=6, resume_free_ram_gb=12)

    low = MemorySnapshot(available_gb=5.9, total_gb=64, used_gb=58.1, source="test")
    middle = MemorySnapshot(available_gb=8.0, total_gb=64, used_gb=56.0, source="test")
    high = MemorySnapshot(available_gb=12.0, total_gb=64, used_gb=52.0, source="test")

    assert should_pause_for_memory(low, cfg)
    assert not should_resume_for_memory(low, cfg)
    assert not should_pause_for_memory(middle, cfg)
    assert not should_resume_for_memory(middle, cfg)
    assert should_resume_for_memory(high, cfg)


def test_active_and_paused_worker_slot_accounting() -> None:
    jobs = [
        ManagedJob("a", 1, 1, "q1", "a", 8, 10, "running", "now"),
        ManagedJob("b", 2, 2, "q2", "b", 4, 9, "paused", "now"),
        ManagedJob("c", 3, 3, "q3", "c", 6, 8, "completed", "now"),
    ]

    assert active_worker_slots(jobs) == 8
    assert paused_worker_slots(jobs) == 4


def test_dequeue_by_id_locks_only_requested_pending_row(tmp_path) -> None:
    db = ResearchDB(tmp_path / "research.sqlite", repo_root=tmp_path)
    db.init_schema()
    first = db.enqueue(
        queue_name="approved_backtests",
        item_type="hypothesis",
        item_id="one",
        priority=100,
        payload={"name": "one", "hypothesis": "one.yaml"},
    )
    second = db.enqueue(
        queue_name="approved_backtests",
        item_type="hypothesis",
        item_id="two",
        priority=10,
        payload={"name": "two", "hypothesis": "two.yaml"},
    )

    row = db.dequeue_by_id("approved_backtests", second, "capacity:test")

    assert row is not None
    assert row["id"] == second
    statuses = {
        str(r["id"]): str(r["status"])
        for r in db.connect().execute("SELECT id, status FROM queues").fetchall()
    }
    assert statuses[first] == "PENDING"
    assert statuses[second] == "LOCKED"
