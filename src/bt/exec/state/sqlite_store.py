from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

from bt.core.enums import OrderState, OrderType, PositionState, Side
from bt.core.types import Fill, Order, Position
from bt.exec.adapters.base import BalanceSnapshot
from bt.exec.state.models import BrokerEventRecord, OrderLifecycleRecord, ProcessedEventRecord, RuntimeCheckpoint, RuntimeSessionState


class SQLiteExecutionStateStore:
    def __init__(self, *, path: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.row_factory = sqlite3.Row
        self._bootstrap_schema()

    def close(self) -> None:
        self._conn.close()

    def _bootstrap_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS session_state (
                run_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                restart_policy TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                ended_at_utc TEXT,
                error TEXT,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_lifecycle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                order_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_order_events_run_order
                ON order_lifecycle_events(run_id, order_id, id);

            CREATE TABLE IF NOT EXISTS processed_events (
                run_id TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                source TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                PRIMARY KEY (run_id, dedupe_key)
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_checkpoints_run_seq
                ON checkpoints(run_id, sequence DESC, id DESC);

            CREATE TABLE IF NOT EXISTS positions_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS balance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            """
        )
        self._conn.commit()

    @staticmethod
    def _to_utc_text(ts: pd.Timestamp) -> str:
        return ts.isoformat()

    @staticmethod
    def _from_utc_text(ts_raw: str) -> pd.Timestamp:
        return pd.Timestamp(ts_raw).tz_convert("UTC")

    def persist_order_lifecycle_event(self, record: OrderLifecycleRecord) -> None:
        self._conn.execute(
            """INSERT INTO order_lifecycle_events(run_id, ts_utc, order_id, event_type, payload_json)
            VALUES (?, ?, ?, ?, ?)""",
            (record.run_id, self._to_utc_text(record.ts), record.order_id, record.event_type, json.dumps(record.payload, sort_keys=True)),
        )
        self._conn.commit()

    def persist_broker_event(self, record: BrokerEventRecord) -> None:
        self.persist_processed_event(
            ProcessedEventRecord(
                ts=record.ts,
                run_id=record.run_id,
                dedupe_key=record.broker_event_id,
                source=record.event_type,
            )
        )

    def persist_processed_event(self, record: ProcessedEventRecord) -> None:
        self._conn.execute(
            """INSERT OR IGNORE INTO processed_events(run_id, dedupe_key, source, ts_utc)
            VALUES (?, ?, ?, ?)""",
            (record.run_id, record.dedupe_key, record.source, self._to_utc_text(record.ts)),
        )
        self._conn.commit()

    def has_processed_event(self, *, run_id: str, dedupe_key: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM processed_events WHERE run_id = ? AND dedupe_key = ?",
            (run_id, dedupe_key),
        ).fetchone()
        return row is not None

    def mark_broker_event_processed(self, dedupe_key: str, ts: pd.Timestamp) -> None:
        run_id = self._latest_run_id()
        if run_id is None:
            return
        self.persist_processed_event(ProcessedEventRecord(ts=ts, run_id=run_id, dedupe_key=dedupe_key, source="broker"))

    def has_processed_broker_event(self, dedupe_key: str) -> bool:
        run_id = self._latest_run_id()
        if run_id is None:
            return False
        return self.has_processed_event(run_id=run_id, dedupe_key=dedupe_key)

    def persist_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        payload = {
            "ts": checkpoint.ts.isoformat(),
            "run_id": checkpoint.run_id,
            "sequence": checkpoint.sequence,
            "last_bar_ts": checkpoint.last_bar_ts.isoformat() if checkpoint.last_bar_ts is not None else None,
            "next_client_order_seq": checkpoint.next_client_order_seq,
            "open_orders": [self._order_to_dict(o) for o in checkpoint.open_orders],
            "positions": [self._position_to_dict(p) for p in checkpoint.positions],
            "balances": self._balance_to_dict(checkpoint.balances),
            "metadata": checkpoint.metadata,
        }
        self._conn.execute(
            """INSERT INTO checkpoints(run_id, ts_utc, sequence, payload_json)
            VALUES (?, ?, ?, ?)""",
            (checkpoint.run_id, self._to_utc_text(checkpoint.ts), checkpoint.sequence, json.dumps(payload, sort_keys=True)),
        )
        self._conn.commit()

    def load_latest_checkpoint(self, run_id: str) -> RuntimeCheckpoint | None:
        row = self._conn.execute(
            "SELECT payload_json FROM checkpoints WHERE run_id = ? ORDER BY sequence DESC, id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        return RuntimeCheckpoint(
            ts=pd.Timestamp(payload["ts"]),
            run_id=str(payload["run_id"]),
            sequence=int(payload["sequence"]),
            last_bar_ts=(None if payload["last_bar_ts"] is None else pd.Timestamp(payload["last_bar_ts"])),
            next_client_order_seq=int(payload.get("next_client_order_seq", 0)),
            open_orders=[self._order_from_dict(o) for o in payload.get("open_orders", [])],
            positions=[self._position_from_dict(p) for p in payload.get("positions", [])],
            balances=self._balance_from_dict(payload.get("balances")),
            metadata=dict(payload.get("metadata") or {}),
        )

    def persist_positions_snapshot(self, *, run_id: str, ts: pd.Timestamp, positions: list[Position]) -> None:
        self._conn.execute(
            "INSERT INTO positions_snapshots(run_id, ts_utc, payload_json) VALUES (?, ?, ?)",
            (run_id, self._to_utc_text(ts), json.dumps([self._position_to_dict(p) for p in positions], sort_keys=True)),
        )
        self._conn.commit()

    def persist_balance_snapshot(self, *, run_id: str, snapshot: BalanceSnapshot) -> None:
        self._conn.execute(
            "INSERT INTO balance_snapshots(run_id, ts_utc, payload_json) VALUES (?, ?, ?)",
            (run_id, self._to_utc_text(snapshot.ts), json.dumps(self._balance_to_dict(snapshot), sort_keys=True)),
        )
        self._conn.commit()

    def query_local_fill_history(self, *, run_id: str, limit: int = 200) -> list[Fill]:
        rows = self._conn.execute(
            """
            SELECT payload_json
            FROM order_lifecycle_events
            WHERE run_id = ? AND event_type IN ('partially_filled', 'filled')
            ORDER BY id DESC
            LIMIT ?
            """,
            (run_id, max(limit, 0)),
        ).fetchall()
        fills: list[Fill] = []
        for row in reversed(rows):
            payload = json.loads(str(row["payload_json"]))
            fill_payload = payload.get("fill")
            if isinstance(fill_payload, dict):
                fills.append(self._fill_from_dict(fill_payload))
        return fills

    def query_open_local_orders(self, *, run_id: str) -> list[Order]:
        rows = self._conn.execute(
            """
            SELECT order_id, event_type, payload_json
            FROM order_lifecycle_events
            WHERE run_id = ?
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()
        order_state: dict[str, Order] = {}
        terminal = {"filled", "cancelled", "rejected", "expired"}
        for row in rows:
            payload = json.loads(str(row["payload_json"]))
            order_payload = payload.get("order")
            order_id = str(row["order_id"])
            if order_payload:
                order_state[order_id] = self._order_from_dict(order_payload)
            elif row["event_type"] in terminal:
                order_state.pop(order_id, None)
        return sorted(order_state.values(), key=lambda o: o.id)

    def query_latest_local_positions_snapshot(self, *, run_id: str) -> list[Position]:
        row = self._conn.execute(
            "SELECT payload_json FROM positions_snapshots WHERE run_id = ? ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is None:
            return []
        payload = json.loads(str(row["payload_json"]))
        return [self._position_from_dict(item) for item in payload]

    def query_latest_balance_snapshot(self, *, run_id: str) -> BalanceSnapshot | None:
        row = self._conn.execute(
            "SELECT payload_json FROM balance_snapshots WHERE run_id = ? ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return self._balance_from_dict(json.loads(str(row["payload_json"])))

    def record_session_liveness(self, session: RuntimeSessionState) -> None:
        self._conn.execute(
            """
            INSERT INTO session_state(run_id, mode, restart_policy, status, started_at_utc, updated_at_utc, ended_at_utc, error, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(run_id) DO UPDATE SET
              status=excluded.status,
              updated_at_utc=excluded.updated_at_utc,
              ended_at_utc=excluded.ended_at_utc,
              metadata_json=excluded.metadata_json
            """,
            (
                session.run_id,
                session.mode,
                session.restart_policy,
                session.status,
                self._to_utc_text(session.started_at),
                self._to_utc_text(session.updated_at),
                None if session.ended_at is None else self._to_utc_text(session.ended_at),
                json.dumps(session.metadata, sort_keys=True),
            ),
        )
        self._conn.commit()

    def mark_session_final_status(self, *, run_id: str, status: str, ts: pd.Timestamp, error: str | None = None) -> None:
        self._conn.execute(
            "UPDATE session_state SET status = ?, updated_at_utc = ?, ended_at_utc = ?, error = COALESCE(?, error) WHERE run_id = ?",
            (status, self._to_utc_text(ts), self._to_utc_text(ts), error, run_id),
        )
        self._conn.commit()

    def load_latest_session(self, *, mode: str) -> RuntimeSessionState | None:
        row = self._conn.execute(
            "SELECT * FROM session_state WHERE mode = ? ORDER BY updated_at_utc DESC LIMIT 1",
            (mode,),
        ).fetchone()
        if row is None:
            return None
        metadata = json.loads(str(row["metadata_json"]))
        return RuntimeSessionState(
            run_id=str(row["run_id"]),
            mode=str(row["mode"]),
            restart_policy=str(row["restart_policy"]),
            status=str(row["status"]),
            started_at=pd.Timestamp(str(row["started_at_utc"])),
            updated_at=pd.Timestamp(str(row["updated_at_utc"])),
            ended_at=None if row["ended_at_utc"] is None else pd.Timestamp(str(row["ended_at_utc"])),
            metadata=metadata,
        )

    def _latest_run_id(self) -> str | None:
        row = self._conn.execute("SELECT run_id FROM session_state ORDER BY updated_at_utc DESC LIMIT 1").fetchone()
        if row is None:
            return None
        return str(row["run_id"])

    @staticmethod
    def _order_to_dict(order: Order) -> dict[str, object]:
        return {
            "id": order.id,
            "ts_submitted": order.ts_submitted.isoformat(),
            "symbol": order.symbol,
            "side": order.side.value,
            "qty": order.qty,
            "order_type": order.order_type.value,
            "limit_price": order.limit_price,
            "state": order.state.value,
            "metadata": order.metadata,
        }

    @staticmethod
    def _order_from_dict(raw: dict[str, object]) -> Order:
        return Order(
            id=str(raw["id"]),
            ts_submitted=pd.Timestamp(raw["ts_submitted"]),
            symbol=str(raw["symbol"]),
            side=Side(str(raw["side"])),
            qty=float(raw["qty"]),
            order_type=OrderType(str(raw["order_type"])),
            limit_price=None if raw.get("limit_price") is None else float(raw["limit_price"]),
            state=OrderState(str(raw["state"])),
            metadata=dict(raw.get("metadata") or {}),
        )

    @staticmethod
    def _position_to_dict(position: Position) -> dict[str, object]:
        return {
            "symbol": position.symbol,
            "state": position.state.value,
            "side": None if position.side is None else position.side.value,
            "qty": position.qty,
            "avg_entry_price": position.avg_entry_price,
            "realized_pnl": position.realized_pnl,
            "unrealized_pnl": position.unrealized_pnl,
            "mae_price": position.mae_price,
            "mfe_price": position.mfe_price,
            "opened_ts": None if position.opened_ts is None else position.opened_ts.isoformat(),
            "closed_ts": None if position.closed_ts is None else position.closed_ts.isoformat(),
        }

    @staticmethod
    def _position_from_dict(raw: dict[str, object]) -> Position:
        return Position(
            symbol=str(raw["symbol"]),
            state=PositionState(str(raw["state"])),
            side=None if raw.get("side") is None else Side(str(raw["side"])),
            qty=float(raw["qty"]),
            avg_entry_price=float(raw["avg_entry_price"]),
            realized_pnl=float(raw["realized_pnl"]),
            unrealized_pnl=float(raw["unrealized_pnl"]),
            mae_price=None if raw.get("mae_price") is None else float(raw["mae_price"]),
            mfe_price=None if raw.get("mfe_price") is None else float(raw["mfe_price"]),
            opened_ts=None if raw.get("opened_ts") is None else pd.Timestamp(raw["opened_ts"]),
            closed_ts=None if raw.get("closed_ts") is None else pd.Timestamp(raw["closed_ts"]),
        )


    @staticmethod
    def _fill_from_dict(raw: dict[str, object]) -> Fill:
        return Fill(
            order_id=str(raw["order_id"]),
            ts=pd.Timestamp(raw["ts"]),
            symbol=str(raw["symbol"]),
            side=Side(str(raw["side"])),
            qty=float(raw["qty"]),
            price=float(raw["price"]),
            fee=float(raw["fee"]),
            slippage=float(raw["slippage"]),
            metadata=dict(raw.get("metadata") or {}),
        )

    @staticmethod
    def _balance_to_dict(snapshot: BalanceSnapshot | None) -> dict[str, object] | None:
        if snapshot is None:
            return None
        return {"ts": snapshot.ts.isoformat(), "balances": snapshot.balances, "metadata": snapshot.metadata}

    @staticmethod
    def _balance_from_dict(raw: object) -> BalanceSnapshot | None:
        if not isinstance(raw, dict):
            return None
        return BalanceSnapshot(
            ts=pd.Timestamp(raw["ts"]),
            balances={str(k): float(v) for k, v in dict(raw.get("balances") or {}).items()},
            metadata=dict(raw.get("metadata") or {}),
        )
