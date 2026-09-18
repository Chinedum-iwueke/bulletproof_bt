import os

import pytest

from bt.experiments.resource_controls import MemorySnapshot
from orchestrator.alpha_capacity_owner import current_owner, owner_alive
from orchestrator.global_capacity_scheduler import (
    CapacitySchedulerConfig, CapacityScheduler, ManagedJob,
    external_locked_worker_slots,
)


def test_owner_binding_rejects_missing_and_reused_pid():
    payload = current_owner()
    assert owner_alive(payload)
    assert not owner_alive({})
    assert not owner_alive({**payload, "owner_start_ticks": "impossible"})


@pytest.fixture
def scheduler(tmp_path):
    cfg = CapacitySchedulerConfig(target_workers=16, max_concurrent_jobs=2,
        log_path=str(tmp_path / "log"), state_path=str(tmp_path / "state"),
        child_log_dir=str(tmp_path / "jobs"))
    value = CapacityScheduler(db_path=tmp_path / "db.sqlite",
        daemon_config_path=tmp_path / "config", daemon_config={}, cfg=cfg)
    yield value
    value.db.close()
    value._leader_lock.close()


def enqueue(scheduler, workers=8):
    return scheduler.db.enqueue(queue_name="approved_backtests", item_type="hypothesis",
        item_id="test", payload={"max_workers": workers})


def test_pending_child_is_not_launched_twice(scheduler):
    queue_id = enqueue(scheduler)
    scheduler.jobs.append(ManagedJob("capacity:test", 123, 123, queue_id, "test", 8, 0, "running", "now"))
    assert scheduler._select_launch_candidate()[0] is None


def test_orphan_capacity_locks_are_counted(scheduler):
    queue_id = enqueue(scheduler)
    scheduler.db.dequeue_by_id("approved_backtests", queue_id, "capacity:old-leader")
    assert external_locked_worker_slots(scheduler.db, "approved_backtests", scheduler.cfg, {}) == 8
    assert external_locked_worker_slots(scheduler.db, "approved_backtests", scheduler.cfg, {}, {"capacity:old-leader"}) == 0


def test_two_eight_worker_jobs_fit_but_third_does_not(scheduler):
    first, second, third = enqueue(scheduler), enqueue(scheduler), enqueue(scheduler)
    scheduler.jobs.append(ManagedJob("capacity:first", 123, 123, first, "a", 8, 0, "running", "now"))
    selected = scheduler._select_launch_candidate()[0]["id"]
    assert selected in {second, third}
    scheduler.jobs.append(ManagedJob("capacity:second", 124, 124, selected, "b", 8, 0, "running", "now"))
    assert scheduler._select_launch_candidate()[0] is None
    assert third != second


def test_missing_memory_and_unrealized_reservations_block_launch(scheduler, monkeypatch):
    enqueue(scheduler)
    assert not scheduler._launch_next_if_capacity(None)
    monkeypatch.setattr(os, "cpu_count", lambda: 32)
    monkeypatch.setattr("orchestrator.global_capacity_scheduler.process_tree_rss_gb", lambda pid: 0.0)
    scheduler.jobs.append(ManagedJob("capacity:first", 123, 123, "other", "a", 8, 0, "running", "now"))
    # 24 GiB would admit one job, but cannot cover another's not-yet-resident reservation.
    assert not scheduler._launch_next_if_capacity(MemorySnapshot(available_gb=24, total_gb=64, used_gb=40, source="test"))


def test_second_leader_is_rejected(scheduler):
    with pytest.raises(RuntimeError, match="already owns"):
        CapacityScheduler(db_path=scheduler.db_path, daemon_config_path=scheduler.daemon_config_path,
            daemon_config={}, cfg=scheduler.cfg)


def test_admission_launches_two_jobs_with_one_shared_budget(scheduler, monkeypatch):
    from types import SimpleNamespace

    enqueue(scheduler)
    enqueue(scheduler)
    enqueue(scheduler)
    commands = []

    def launch(command, **options):
        commands.append(command)
        queue_id = command[command.index("--queue-id") + 1]
        owner = command[command.index("--locked-by") + 1]
        assert scheduler.db.dequeue_by_id("approved_backtests", queue_id, owner)
        assert options["start_new_session"]
        return SimpleNamespace(pid=10000 + len(commands))

    monkeypatch.setattr("orchestrator.global_capacity_scheduler.subprocess.Popen", launch)
    monkeypatch.setattr(os, "cpu_count", lambda: 36)
    monkeypatch.setattr(os, "getloadavg", lambda: (0, 0, 0))
    monkeypatch.setattr("orchestrator.global_capacity_scheduler.process_tree_rss_gb", lambda pid: 0.0)
    memory = MemorySnapshot(available_gb=64, total_gb=72, used_gb=8, source="test")
    assert scheduler._launch_next_if_capacity(memory)
    assert scheduler._launch_next_if_capacity(memory)
    assert not scheduler._launch_next_if_capacity(memory)
    assert len(commands) == 2
    assert sum(job.estimated_workers for job in scheduler.jobs) == 16


def test_completed_child_is_reaped_before_expired_owner_can_overwrite_done(scheduler, monkeypatch):
    queue_id = scheduler.db.enqueue(
        queue_name="approved_backtests",
        item_type="governed_alpha_assignment",
        item_id="terminal",
        payload={
            "kind": "governed_alpha_assignment",
            "owner_pid": 999999,
            "owner_start_ticks": "expired",
            "max_workers": 8,
        },
    )
    scheduler.db.dequeue_by_id("approved_backtests", queue_id, "capacity:test")
    scheduler.db.mark_queue_failed(queue_id, "stale race")
    scheduler.db.mark_queue_done(queue_id)
    scheduler.jobs.append(
        ManagedJob(
            "capacity:test", 123, 123, queue_id, "terminal", 8, 0, "running", "now"
        )
    )
    monkeypatch.setattr(os, "waitpid", lambda pid, flags: (pid, 0))
    killed = []
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: killed.append((pgid, sig)))

    scheduler._reap_jobs()

    row = scheduler.db.connect().execute(
        "SELECT status, last_error FROM queues WHERE id = ?", (queue_id,)
    ).fetchone()
    assert dict(row) == {"status": "DONE", "last_error": None}
    assert scheduler.jobs == []
    assert killed == []
