from __future__ import annotations

import pandas as pd

from bt.exec.lifecycle import build_client_order_id, is_valid_client_order_id, lifecycle_dedupe_key, should_process_once
from bt.exec.state.models import ProcessedEventRecord
from bt.exec.state.sqlite_store import SQLiteExecutionStateStore


def test_client_order_id_generation_and_validation() -> None:
    cid = build_client_order_id(order_seq=12)
    assert cid == "co-12"
    assert is_valid_client_order_id(cid)
    assert not is_valid_client_order_id("bad-12")


def test_lifecycle_dedupe_and_store_once(tmp_path) -> None:
    store = SQLiteExecutionStateStore(path=str(tmp_path / "state.sqlite"))
    key = lifecycle_dedupe_key(order_id="o1", event_type="filled")
    assert should_process_once(store=store, run_id="run-1", dedupe_key=key)
    store.persist_processed_event(ProcessedEventRecord(ts=pd.Timestamp("2026-01-01T00:00:00Z"), run_id="run-1", dedupe_key=key, source="test"))
    assert not should_process_once(store=store, run_id="run-1", dedupe_key=key)
