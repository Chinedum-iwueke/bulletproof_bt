"""Lightweight SQLite research database helpers for orchestration workflows."""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any
from uuid import uuid4

HYPOTHESIS_STATUSES = {
    "IDEA",
    "EXPANDED",
    "GRID_DESIGNED",
    "IMPLEMENTATION_PENDING",
    "IMPLEMENTED",
    "QA_PASSED",
    "MANIFEST_BUILT",
    "RUNNING",
    "RUN_COMPLETE",
    "POST_ANALYSIS_COMPLETE",
    "DATASET_EXTRACTED",
    "CLEANED",
    "INTERPRETED",
    "AWAITING_DECISION",
    "SCRAPPED",
    "REFINED",
    "PROMOTED_TIER3",
    "PROMOTED_FORWARD_TEST",
    "DEPLOYED",
}

EXPERIMENT_STATUSES = {
    "PENDING",
    "MANIFEST_BUILT",
    "RUNNING",
    "RUN_COMPLETE",
    "POST_ANALYSIS_COMPLETE",
    "DATASET_EXTRACTED",
    "CLEANED",
    "FAILED",
}

PIPELINE_RUN_STATUSES = {
    "STARTED",
    "BUILDING_MANIFESTS",
    "RUNNING_BACKTESTS",
    "POST_ANALYSIS",
    "EXTRACTING_DATASETS",
    "CLEANING",
    "CREATING_VERDICT_BUNDLE",
    "COMPLETED",
    "FAILED",
}

QUEUE_STATUSES = {"PENDING", "LOCKED", "DONE", "FAILED", "CANCELLED", "WAITING_FOR_APPROVAL"}
VERDICT_VALUES = {
    "SCRAP",
    "REFINE_ENTRY",
    "REFINE_EXIT",
    "REFINE_GATE",
    "REFINE_TIMEFRAME",
    "ADD_STATE_FILTER",
    "PROMOTE_TIER3",
    "PROMOTE_FORWARD_TEST",
    "ADD_TO_ALPHA_ZOO",
    "NEEDS_MORE_DATA",
    "INCONCLUSIVE",
}


class ResearchDB:
    def __init__(self, db_path: str | Path, repo_root: str | Path | None = None) -> None:
        self.db_path = Path(db_path)
        self.repo_root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[1]
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode = WAL;")
            self._conn = conn
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def init_schema(self) -> None:
        conn = self.connect()
        schema_path = Path(__file__).resolve().with_name("research_db_schema.sql")
        schema_sql = schema_path.read_text(encoding="utf-8")
        conn.executescript(schema_sql)
        conn.commit()

    def normalize_path(self, path: str | Path | None) -> str | None:
        if path is None:
            return None
        p = Path(path)
        if not p.is_absolute():
            p = (self.repo_root / p).resolve()
        else:
            p = p.resolve()

        try:
            return str(p.relative_to(self.repo_root))
        except ValueError:
            return str(p)

    def _now(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def _new_id(self) -> str:
        return str(uuid4())

    def _json(self, value: Any) -> str | None:
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    def _require_status(self, value: str, allowed: set[str], field: str) -> None:
        if value not in allowed:
            raise ValueError(f"Invalid {field}={value}; allowed={sorted(allowed)}")

    def create_hypothesis(
        self,
        *,
        name: str,
        status: str = "IDEA",
        layer: str | None = None,
        family: str | None = None,
        yaml_path: str | Path | None = None,
        priority: int = 50,
        parent_hypothesis_id: str | None = None,
        notes: str | None = None,
        metadata: Any = None,
    ) -> str:
        self._require_status(status, HYPOTHESIS_STATUSES, "hypothesis_status")
        now = self._now()
        hid = self._new_id()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO hypotheses (
                id, name, layer, family, yaml_path, status, priority, parent_hypothesis_id,
                created_at, updated_at, notes, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                hid,
                name,
                layer,
                family,
                self.normalize_path(yaml_path),
                status,
                priority,
                parent_hypothesis_id,
                now,
                now,
                notes,
                self._json(metadata),
            ),
        )
        conn.commit()
        return hid

    def get_hypothesis(self, hypothesis_id: str) -> sqlite3.Row | None:
        conn = self.connect()
        return conn.execute("SELECT * FROM hypotheses WHERE id = ?", (hypothesis_id,)).fetchone()

    def get_hypothesis_by_name(self, name: str) -> sqlite3.Row | None:
        conn = self.connect()
        return conn.execute("SELECT * FROM hypotheses WHERE name = ? ORDER BY created_at DESC LIMIT 1", (name,)).fetchone()

    def update_hypothesis_status(self, hypothesis_id: str, status: str, notes: str | None = None) -> None:
        self._require_status(status, HYPOTHESIS_STATUSES, "hypothesis_status")
        now = self._now()
        conn = self.connect()
        conn.execute(
            "UPDATE hypotheses SET status = ?, notes = COALESCE(?, notes), updated_at = ? WHERE id = ?",
            (status, notes, now, hypothesis_id),
        )
        conn.commit()

    def upsert_hypothesis_by_name(
        self,
        *,
        name: str,
        yaml_path: str | Path | None = None,
        status: str = "IDEA",
        layer: str | None = None,
        family: str | None = None,
        priority: int = 50,
        notes: str | None = None,
        metadata: Any = None,
    ) -> str:
        existing = self.get_hypothesis_by_name(name)
        if existing is None:
            return self.create_hypothesis(
                name=name,
                status=status,
                layer=layer,
                family=family,
                yaml_path=yaml_path,
                priority=priority,
                notes=notes,
                metadata=metadata,
            )

        self._require_status(status, HYPOTHESIS_STATUSES, "hypothesis_status")
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            UPDATE hypotheses
            SET yaml_path = COALESCE(?, yaml_path),
                status = ?,
                layer = COALESCE(?, layer),
                family = COALESCE(?, family),
                priority = ?,
                notes = COALESCE(?, notes),
                metadata_json = COALESCE(?, metadata_json),
                updated_at = ?
            WHERE id = ?
            """,
            (
                self.normalize_path(yaml_path),
                status,
                layer,
                family,
                priority,
                notes,
                self._json(metadata),
                now,
                str(existing["id"]),
            ),
        )
        conn.commit()
        return str(existing["id"])

    def create_experiment(
        self,
        *,
        hypothesis_id: str,
        name: str,
        phase: str,
        dataset_type: str,
        experiment_root: str | Path,
        status: str = "PENDING",
        manifest_path: str | Path | None = None,
        max_workers: int | None = None,
        config_path: str | Path | None = None,
        local_config_path: str | Path | None = None,
        data_path: str | Path | None = None,
        metadata: Any = None,
    ) -> str:
        if dataset_type not in {"stable", "volatile"}:
            raise ValueError("dataset_type must be one of: stable, volatile")
        self._require_status(status, EXPERIMENT_STATUSES, "experiment_status")

        now = self._now()
        exp_id = self._new_id()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO experiments (
                id, hypothesis_id, name, phase, dataset_type, experiment_root, manifest_path, status,
                started_at, completed_at, created_at, updated_at, max_workers, config_path,
                local_config_path, data_path, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                exp_id,
                hypothesis_id,
                name,
                phase,
                dataset_type,
                self.normalize_path(experiment_root),
                self.normalize_path(manifest_path),
                status,
                None,
                None,
                now,
                now,
                max_workers,
                self.normalize_path(config_path),
                self.normalize_path(local_config_path),
                self.normalize_path(data_path),
                self._json(metadata),
            ),
        )
        conn.commit()
        return exp_id

    def update_experiment(self, experiment_id: str, **fields: Any) -> None:
        if not fields:
            return
        now = self._now()
        mapping = {
            "manifest_path": self.normalize_path(fields.get("manifest_path")) if "manifest_path" in fields else None,
            "experiment_root": self.normalize_path(fields.get("experiment_root")) if "experiment_root" in fields else None,
            "config_path": self.normalize_path(fields.get("config_path")) if "config_path" in fields else None,
            "local_config_path": self.normalize_path(fields.get("local_config_path")) if "local_config_path" in fields else None,
            "data_path": self.normalize_path(fields.get("data_path")) if "data_path" in fields else None,
            "max_workers": fields.get("max_workers"),
            "metadata_json": self._json(fields.get("metadata")) if "metadata" in fields else None,
            "started_at": fields.get("started_at"),
            "completed_at": fields.get("completed_at"),
            "status": fields.get("status"),
        }

        if mapping.get("status") is not None:
            self._require_status(str(mapping["status"]), EXPERIMENT_STATUSES, "experiment_status")

        updates: list[str] = []
        values: list[Any] = []
        for key, value in mapping.items():
            if key in fields or (key == "metadata_json" and "metadata" in fields):
                updates.append(f"{key} = ?")
                values.append(value)
        updates.append("updated_at = ?")
        values.append(now)
        values.append(experiment_id)

        conn = self.connect()
        conn.execute(f"UPDATE experiments SET {', '.join(updates)} WHERE id = ?", tuple(values))
        conn.commit()

    def update_experiment_status(self, experiment_id: str, status: str) -> None:
        self._require_status(status, EXPERIMENT_STATUSES, "experiment_status")
        now = self._now()
        started_at = now if status == "RUNNING" else None
        completed_at = now if status in {"RUN_COMPLETE", "POST_ANALYSIS_COMPLETE", "DATASET_EXTRACTED", "CLEANED", "FAILED"} else None

        conn = self.connect()
        if started_at and completed_at:
            conn.execute(
                "UPDATE experiments SET status = ?, started_at = COALESCE(started_at, ?), completed_at = ?, updated_at = ? WHERE id = ?",
                (status, started_at, completed_at, now, experiment_id),
            )
        elif started_at:
            conn.execute(
                "UPDATE experiments SET status = ?, started_at = COALESCE(started_at, ?), updated_at = ? WHERE id = ?",
                (status, started_at, now, experiment_id),
            )
        elif completed_at:
            conn.execute(
                "UPDATE experiments SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?",
                (status, completed_at, now, experiment_id),
            )
        else:
            conn.execute(
                "UPDATE experiments SET status = ?, updated_at = ? WHERE id = ?",
                (status, now, experiment_id),
            )
        conn.commit()

    def create_pipeline_run(
        self,
        *,
        name: str,
        phase: str,
        hypothesis_path: str | Path,
        status: str = "STARTED",
        hypothesis_id: str | None = None,
        stable_experiment_id: str | None = None,
        volatile_experiment_id: str | None = None,
        log_path: str | Path | None = None,
        commands: Any = None,
        metadata: Any = None,
    ) -> str:
        self._require_status(status, PIPELINE_RUN_STATUSES, "pipeline_status")
        run_id = self._new_id()
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO pipeline_runs (
                id, hypothesis_id, name, phase, status, started_at, completed_at, hypothesis_path,
                stable_experiment_id, volatile_experiment_id, verdict_bundle_path, log_path,
                error_message, commands_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                hypothesis_id,
                name,
                phase,
                status,
                now,
                None,
                self.normalize_path(hypothesis_path),
                stable_experiment_id,
                volatile_experiment_id,
                None,
                self.normalize_path(log_path),
                None,
                self._json(commands),
                self._json(metadata),
            ),
        )
        conn.commit()
        return run_id

    def update_pipeline_run_status(self, pipeline_run_id: str, status: str, commands: Any = None) -> None:
        self._require_status(status, PIPELINE_RUN_STATUSES, "pipeline_status")
        now = self._now()
        completed_at = now if status == "COMPLETED" else None
        conn = self.connect()
        conn.execute(
            """
            UPDATE pipeline_runs
            SET status = ?,
                commands_json = COALESCE(?, commands_json),
                completed_at = COALESCE(?, completed_at)
            WHERE id = ?
            """,
            (status, self._json(commands), completed_at, pipeline_run_id),
        )
        conn.commit()

    def complete_pipeline_run(
        self,
        pipeline_run_id: str,
        *,
        verdict_bundle_path: str | Path | None = None,
        commands: Any = None,
    ) -> None:
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            UPDATE pipeline_runs
            SET status = 'COMPLETED', completed_at = ?, verdict_bundle_path = COALESCE(?, verdict_bundle_path),
                commands_json = COALESCE(?, commands_json)
            WHERE id = ?
            """,
            (now, self.normalize_path(verdict_bundle_path), self._json(commands), pipeline_run_id),
        )
        conn.commit()

    def fail_pipeline_run(self, pipeline_run_id: str, error_message: str, commands: Any = None) -> None:
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            UPDATE pipeline_runs
            SET status = 'FAILED', completed_at = ?, error_message = ?, commands_json = COALESCE(?, commands_json)
            WHERE id = ?
            """,
            (now, error_message, self._json(commands), pipeline_run_id),
        )
        conn.commit()

    def register_artifact(
        self,
        *,
        artifact_type: str,
        path: str | Path,
        hypothesis_id: str | None = None,
        experiment_id: str | None = None,
        pipeline_run_id: str | None = None,
        description: str | None = None,
        metadata: Any = None,
    ) -> str:
        artifact_id = self._new_id()
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO artifacts (
                id, hypothesis_id, experiment_id, pipeline_run_id, artifact_type, path, description, created_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                hypothesis_id,
                experiment_id,
                pipeline_run_id,
                artifact_type,
                self.normalize_path(path),
                description,
                now,
                self._json(metadata),
            ),
        )
        conn.commit()
        return artifact_id

    def list_artifacts(
        self,
        *,
        hypothesis_id: str | None = None,
        experiment_id: str | None = None,
        pipeline_run_id: str | None = None,
    ) -> list[sqlite3.Row]:
        clauses = []
        values: list[Any] = []
        if hypothesis_id:
            clauses.append("hypothesis_id = ?")
            values.append(hypothesis_id)
        if experiment_id:
            clauses.append("experiment_id = ?")
            values.append(experiment_id)
        if pipeline_run_id:
            clauses.append("pipeline_run_id = ?")
            values.append(pipeline_run_id)

        query = "SELECT * FROM artifacts"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC"
        return self.connect().execute(query, tuple(values)).fetchall()

    def enqueue(
        self,
        *,
        queue_name: str,
        item_type: str,
        item_id: str,
        status: str = "PENDING",
        priority: int = 50,
        payload: Any = None,
        available_after: str | None = None,
    ) -> str:
        self._require_status(status, QUEUE_STATUSES, "queue_status")
        queue_id = self._new_id()
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO queues (
                id, queue_name, item_type, item_id, status, priority, payload_json,
                created_at, updated_at, available_after, locked_at, locked_by, attempts, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (queue_id, queue_name, item_type, item_id, status, priority, self._json(payload), now, now, available_after, None, None, 0, None),
        )
        conn.commit()
        return queue_id

    def dequeue_next(self, queue_name: str, locked_by: str) -> sqlite3.Row | None:
        now = self._now()
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT * FROM queues
            WHERE queue_name = ?
              AND status = 'PENDING'
              AND (available_after IS NULL OR available_after <= ?)
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            """,
            (queue_name, now),
        ).fetchone()
        if row is None:
            conn.commit()
            return None

        conn.execute(
            """
            UPDATE queues
            SET status = 'LOCKED', locked_at = ?, locked_by = ?, attempts = attempts + 1, updated_at = ?
            WHERE id = ?
            """,
            (now, locked_by, now, row["id"]),
        )
        conn.commit()
        return conn.execute("SELECT * FROM queues WHERE id = ?", (row["id"],)).fetchone()

    def peek_next_pending(self, queue_name: str) -> sqlite3.Row | None:
        now = self._now()
        return self.connect().execute(
            """
            SELECT * FROM queues
            WHERE queue_name = ?
              AND status = 'PENDING'
              AND (available_after IS NULL OR available_after <= ?)
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            """,
            (queue_name, now),
        ).fetchone()

    def mark_queue_done(self, queue_id: str) -> None:
        now = self._now()
        conn = self.connect()
        conn.execute(
            "UPDATE queues SET status = 'DONE', updated_at = ?, locked_at = NULL, locked_by = NULL WHERE id = ?",
            (now, queue_id),
        )
        conn.commit()

    def mark_queue_failed(self, queue_id: str, error: str) -> None:
        now = self._now()
        conn = self.connect()
        conn.execute(
            "UPDATE queues SET status = 'FAILED', updated_at = ?, last_error = ?, locked_at = NULL, locked_by = NULL WHERE id = ?",
            (now, error, queue_id),
        )
        conn.commit()

    def release_queue_lock(self, queue_id: str, error: str | None = None) -> None:
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            UPDATE queues
            SET status = 'PENDING',
                updated_at = ?,
                locked_at = NULL,
                locked_by = NULL,
                last_error = COALESCE(?, last_error)
            WHERE id = ? AND status = 'LOCKED'
            """,
            (now, error, queue_id),
        )
        conn.commit()

    def release_stale_locks(self, queue_name: str, older_than_minutes: int, max_job_attempts: int) -> dict[str, int]:
        now = datetime.now(timezone.utc)
        threshold = (now - timedelta(minutes=older_than_minutes)).replace(microsecond=0).isoformat()
        conn = self.connect()
        stale_rows = conn.execute(
            """
            SELECT id, attempts FROM queues
            WHERE queue_name = ?
              AND status = 'LOCKED'
              AND locked_at IS NOT NULL
              AND locked_at <= ?
            """,
            (queue_name, threshold),
        ).fetchall()

        requeued = 0
        failed = 0
        now_iso = now.replace(microsecond=0).isoformat()
        for row in stale_rows:
            queue_id = str(row["id"])
            attempts = int(row["attempts"] or 0)
            if attempts < max_job_attempts:
                conn.execute(
                    """
                    UPDATE queues
                    SET status = 'PENDING',
                        updated_at = ?,
                        locked_at = NULL,
                        locked_by = NULL,
                        last_error = 'stale lock released'
                    WHERE id = ?
                    """,
                    (now_iso, queue_id),
                )
                requeued += 1
            else:
                conn.execute(
                    """
                    UPDATE queues
                    SET status = 'FAILED',
                        updated_at = ?,
                        locked_at = NULL,
                        locked_by = NULL,
                        last_error = 'stale lock exceeded max attempts'
                    WHERE id = ?
                    """,
                    (now_iso, queue_id),
                )
                failed += 1
        conn.commit()
        return {"requeued": requeued, "failed": failed}

    def create_verdict(
        self,
        *,
        hypothesis_id: str,
        verdict: str,
        pipeline_run_id: str | None = None,
        confidence: float | None = None,
        summary: str | None = None,
        evidence: Any = None,
        recommended_next_action: str | None = None,
        next_hypothesis_id: str | None = None,
        memo_path: str | Path | None = None,
        approved_by_user: int = 0,
    ) -> str:
        if verdict not in VERDICT_VALUES:
            raise ValueError(f"Invalid verdict={verdict}; allowed={sorted(VERDICT_VALUES)}")
        verdict_id = self._new_id()
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO verdicts (
                id, hypothesis_id, pipeline_run_id, verdict, confidence, summary, evidence_json,
                recommended_next_action, next_hypothesis_id, memo_path, approved_by_user, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id,
                hypothesis_id,
                pipeline_run_id,
                verdict,
                confidence,
                summary,
                self._json(evidence),
                recommended_next_action,
                next_hypothesis_id,
                self.normalize_path(memo_path),
                approved_by_user,
                now,
                now,
            ),
        )
        conn.commit()
        return verdict_id

    def create_state_finding(
        self,
        *,
        state_variable: str,
        bucket: str,
        hypothesis_id: str | None = None,
        experiment_id: str | None = None,
        dataset_type: str | None = None,
        n_trades: int | None = None,
        ev_r_net: float | None = None,
        median_r: float | None = None,
        p95_r: float | None = None,
        p99_r: float | None = None,
        max_r: float | None = None,
        min_r: float | None = None,
        notes: str | None = None,
        evidence: Any = None,
    ) -> str:
        finding_id = self._new_id()
        now = self._now()
        conn = self.connect()
        conn.execute(
            """
            INSERT INTO state_findings (
                id, hypothesis_id, experiment_id, state_variable, bucket, dataset_type,
                n_trades, ev_r_net, median_r, p95_r, p99_r, max_r, min_r,
                notes, evidence_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                finding_id,
                hypothesis_id,
                experiment_id,
                state_variable,
                bucket,
                dataset_type,
                n_trades,
                ev_r_net,
                median_r,
                p95_r,
                p99_r,
                max_r,
                min_r,
                notes,
                self._json(evidence),
                now,
                now,
            ),
        )
        conn.commit()
        return finding_id

    def import_runs_from_summary_csv(self, experiment_id: str, summary_csv_path: str | Path) -> int:
        path = Path(summary_csv_path)
        if not path.exists():
            raise FileNotFoundError(f"Summary CSV not found: {path}")

        def parse_float(v: str | None) -> float | None:
            if v is None or v == "":
                return None
            try:
                return float(v)
            except ValueError:
                return None

        def parse_int(v: str | None) -> int | None:
            if v is None or v == "":
                return None
            try:
                return int(float(v))
            except ValueError:
                return None

        imported = 0
        conn = self.connect()
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                run_id = (row.get("run_id") or "").strip()
                if not run_id:
                    continue
                now = self._now()
                conn.execute(
                    """
                    INSERT INTO runs (
                        id, experiment_id, run_id, run_path, config_hash, status,
                        ev_r_net, ev_r_gross, n_trades, win_rate, max_drawdown, max_drawdown_duration,
                        tail_5r_count, tail_10r_count, avg_r_win, avg_r_loss, summary_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(experiment_id, run_id)
                    DO UPDATE SET
                        run_path = excluded.run_path,
                        config_hash = excluded.config_hash,
                        status = excluded.status,
                        ev_r_net = excluded.ev_r_net,
                        ev_r_gross = excluded.ev_r_gross,
                        n_trades = excluded.n_trades,
                        win_rate = excluded.win_rate,
                        max_drawdown = excluded.max_drawdown,
                        max_drawdown_duration = excluded.max_drawdown_duration,
                        tail_5r_count = excluded.tail_5r_count,
                        tail_10r_count = excluded.tail_10r_count,
                        avg_r_win = excluded.avg_r_win,
                        avg_r_loss = excluded.avg_r_loss,
                        summary_json = excluded.summary_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        self._new_id(),
                        experiment_id,
                        run_id,
                        row.get("run_path"),
                        row.get("config_hash"),
                        row.get("status"),
                        parse_float(row.get("ev_r_net")),
                        parse_float(row.get("ev_r_gross")),
                        parse_int(row.get("n_trades")),
                        parse_float(row.get("win_rate")),
                        parse_float(row.get("max_drawdown")),
                        parse_float(row.get("max_drawdown_duration")),
                        parse_int(row.get("tail_5r_count")),
                        parse_int(row.get("tail_10r_count")),
                        parse_float(row.get("avg_r_win")),
                        parse_float(row.get("avg_r_loss")),
                        self._json(row),
                        now,
                        now,
                    ),
                )
                imported += 1
        conn.commit()
        return imported
