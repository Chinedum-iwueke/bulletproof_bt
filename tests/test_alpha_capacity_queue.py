import hashlib
import json
from types import SimpleNamespace

import pytest

from orchestrator.alpha_capacity_owner import current_owner
from orchestrator.db import ResearchDB
from scripts import run_alpha_capacity_job as job
from scripts import queue_alpha_capacity_assignment as wrapper


def setup_job(tmp_path, monkeypatch, **changes):
    assignment = tmp_path / "assignment.json"
    assignment.write_text('{"stage":"execute"}')
    database = tmp_path / "capacity.sqlite"
    db = ResearchDB(database)
    db.init_schema()
    payload = {"kind": "governed_alpha_assignment", "max_workers": 8,
        "assignment": str(assignment), "assignment_sha256": hashlib.sha256(assignment.read_bytes()).hexdigest(),
        "repository_root": str(tmp_path), "output": str(tmp_path / "output"),
        "receipt": str(tmp_path / "receipt"), **current_owner(), **changes}
    queue_id = db.enqueue(queue_name="approved_backtests", item_type="governed_alpha_assignment", item_id="test", payload=payload)
    monkeypatch.setattr("sys.argv", ["job", "--db", str(database), "--queue-id", queue_id,
        "--queue-name", "approved_backtests", "--locked-by", "capacity:test"])
    return db, queue_id, assignment


@pytest.mark.parametrize("failure", ["owner", "tamper"])
def test_job_refuses_dead_owner_or_changed_assignment(tmp_path, monkeypatch, failure):
    changes = {"owner_start_ticks": "wrong"} if failure == "owner" else {}
    db, queue_id, assignment = setup_job(tmp_path, monkeypatch, **changes)
    if failure == "tamper":
        assignment.write_text("changed")
    monkeypatch.setattr(job.subprocess, "run", lambda *a, **kw: pytest.fail("Must not execute"))
    with pytest.raises(ValueError):
        job.main()
    assert db.connect().execute("SELECT status FROM queues WHERE id=?", (queue_id,)).fetchone()["status"] == "FAILED"
    db.close()


def test_dispatch_uses_assignment_repository_and_exact_worker_count(tmp_path, monkeypatch):
    db, queue_id, _ = setup_job(tmp_path, monkeypatch)
    commands = []
    monkeypatch.setattr(job.subprocess, "run", lambda command, **kw: commands.append((command, kw)) or SimpleNamespace(returncode=0))
    assert job.main() == 0
    command, options = commands[0]
    assert command[1] == str(tmp_path / "scripts/run_alpha_research_assignment.py")
    assert command[-2:] == ["--max-workers", "8"]
    assert options["cwd"] == tmp_path
    assert db.connect().execute("SELECT status FROM queues WHERE id=?", (queue_id,)).fetchone()["status"] == "DONE"
    # Terminal queue rows cannot be claimed again.
    assert job.main() == 0
    assert len(commands) == 1
    db.close()


def test_wrapper_refuses_duplicate_immutable_assignment(tmp_path, monkeypatch):
    database = tmp_path / "db"
    assignment = tmp_path / "assignment"
    assignment.write_text(json.dumps({"stage": "execute", "max_variants": 8, "question_digest": "a" * 64}))
    db = ResearchDB(database)
    db.init_schema()
    db.enqueue(queue_name="approved_backtests", item_type="governed_alpha_assignment",
               item_id=hashlib.sha256(assignment.read_bytes()).hexdigest(), payload={})
    db.close()
    monkeypatch.setattr("sys.argv", ["wrapper", "--db", str(database), "--assignment", str(assignment),
        "--repository-root", str(tmp_path), "--output", str(tmp_path / "output"), "--receipt", str(tmp_path / "receipt")])
    with pytest.raises(RuntimeError, match="already entered"):
        wrapper.main()
