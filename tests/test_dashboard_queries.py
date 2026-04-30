from pathlib import Path

from orchestrator.dashboard.artifact_reader import safe_resolve_artifact
from orchestrator.dashboard.db_queries import get_alpha_zoo, get_queue, get_summary, set_approval
from orchestrator.db import ResearchDB


def make_db(tmp_path: Path):
    dbp = tmp_path / 'r.sqlite'
    db = ResearchDB(dbp, repo_root=tmp_path)
    db.init_schema()
    return db


def test_summary_empty_db(tmp_path: Path):
    db = make_db(tmp_path)
    s = get_summary(db.connect())
    assert s['failed_jobs'] == 0


def test_queue_query_group_status(tmp_path: Path):
    db = make_db(tmp_path)
    db.enqueue(queue_name='q', item_type='t', item_id='1', status='PENDING', priority=1)
    db.enqueue(queue_name='q', item_type='t', item_id='2', status='FAILED', priority=1)
    assert len(get_queue(db.connect(), status='FAILED')) == 1


def test_missing_alpha_candidates_no_crash(tmp_path: Path):
    db = make_db(tmp_path)
    assert get_alpha_zoo(db.connect(), tmp_path) == []


def test_path_sanitizer_blocks_path_traversal(tmp_path: Path):
    (tmp_path / 'logs').mkdir()
    try:
        safe_resolve_artifact(tmp_path, '../etc/passwd')
        assert False
    except ValueError:
        assert True


def test_approval_updates_queue(tmp_path: Path):
    db = make_db(tmp_path)
    qid = db.enqueue(queue_name='approval_queue', item_type='verdict', item_id='v1', status='WAITING_FOR_APPROVAL', payload={'verdict_id':'vX'})
    ok = set_approval(db.connect(), qid, approve=False)
    assert ok
    row = db.connect().execute('select status from queues where id=?', (qid,)).fetchone()
    assert row[0] == 'CANCELLED'
