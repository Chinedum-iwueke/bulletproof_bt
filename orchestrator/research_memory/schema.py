from __future__ import annotations

import sqlite3


def ensure_research_memory_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_memory_trades (
            id TEXT PRIMARY KEY,
            source_trade_id TEXT,
            run_id TEXT,
            hypothesis_id TEXT,
            hypothesis_name TEXT,
            strategy_id TEXT,
            setup_class TEXT,
            symbol TEXT,
            dataset_type TEXT,
            phase TEXT,
            research_tier TEXT,
            research_mode TEXT,
            evidence_type TEXT,
            portfolio_constraints_applied INTEGER,
            capital_path_valid INTEGER,
            deployability_evidence INTEGER,
            signal_episode_evidence INTEGER,
            experiment_root TEXT,
            ts_signal TEXT,
            ts_entry_fill TEXT,
            ts_exit_fill TEXT,
            r_net REAL,
            r_gross REAL,
            pnl_net REAL,
            pnl_gross REAL,
            mfe_r REAL,
            mae_r REAL,
            exit_efficiency REAL,
            cost_drag_r REAL,
            fee_drag_r REAL,
            slippage_drag_r REAL,
            spread_drag_r REAL,
            csi_pctile REAL,
            csi_source TEXT,
            csi_components_json TEXT,
            vol_pctile REAL,
            spread_pctile REAL,
            tr_over_atr REAL,
            tr_over_atr_pctile REAL,
            volume_pctile REAL,
            vol_of_vol_pctile REAL,
            funding_raw REAL,
            funding_pctile REAL,
            funding_z REAL,
            oi_level REAL,
            oi_accel REAL,
            oi_accel_pctile REAL,
            oi_z REAL,
            mark_price REAL,
            index_price REAL,
            basis_raw REAL,
            basis_pct REAL,
            basis_pctile REAL,
            premium_pctile REAL,
            crowding_proxy_pctile REAL,
            constraint_stress_pctile REAL,
            trend_state TEXT,
            vol_regime TEXT,
            liquidity_regime TEXT,
            displacement_regime TEXT,
            market_regime_class TEXT,
            structure_class TEXT,
            label_success_1r_before_neg_1r INTEGER,
            label_success_2r_before_neg_1r INTEGER,
            label_reached_3r INTEGER,
            label_reached_5r INTEGER,
            label_tail_trade_ge_10r INTEGER,
            label_profitable_after_costs INTEGER,
            label_high_cost_trade INTEGER,
            metrics_valid INTEGER DEFAULT 1,
            invalid_reason TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(experiment_root, run_id, source_trade_id)
        );

        CREATE TABLE IF NOT EXISTS research_memory_state_buckets (
            id TEXT PRIMARY KEY,
            state_key TEXT NOT NULL,
            bucket TEXT NOT NULL,
            joint_key TEXT,
            setup_class TEXT,
            hypothesis_name TEXT,
            dataset_type TEXT,
            phase TEXT,
            n_trades INTEGER,
            ev_r_net REAL,
            ev_r_gross REAL,
            median_r REAL,
            p95_r REAL,
            p99_r REAL,
            max_r REAL,
            min_r REAL,
            tail_3r_rate REAL,
            tail_5r_rate REAL,
            tail_10r_rate REAL,
            avg_mfe_r REAL,
            avg_mae_r REAL,
            avg_exit_efficiency REAL,
            avg_cost_drag_r REAL,
            finding_type TEXT,
            confidence_score REAL,
            evidence_json TEXT,
            source_path TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS research_memory_candidates (
            id TEXT PRIMARY KEY,
            candidate_id TEXT,
            hypothesis_name TEXT,
            run_id TEXT,
            dataset_type TEXT,
            phase TEXT,
            candidate_status TEXT,
            rank_score REAL,
            promotion_score REAL,
            ev_r_net REAL,
            n_trades INTEGER,
            tail_5r_count INTEGER,
            tail_10r_count INTEGER,
            setup_class TEXT,
            state_profile_json TEXT,
            recommended_action TEXT,
            source_path TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS research_memory_recommendations (
            id TEXT PRIMARY KEY,
            recommendation_type TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_id TEXT,
            hypothesis_name TEXT,
            setup_class TEXT,
            state_condition_json TEXT,
            recommendation TEXT NOT NULL,
            evidence_score REAL,
            confidence REAL,
            status TEXT NOT NULL,
            human_approved INTEGER DEFAULT 0,
            evidence_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS research_memory_publications (
            publication_key TEXT PRIMARY KEY,
            bundle_digest TEXT NOT NULL UNIQUE,
            request_digest TEXT NOT NULL UNIQUE,
            trial_id TEXT NOT NULL,
            result_id TEXT NOT NULL,
            canonical_receipt_json TEXT NOT NULL,
            record_digest TEXT NOT NULL UNIQUE
        );

        CREATE INDEX IF NOT EXISTS idx_rm_trades_setup ON research_memory_trades(setup_class);
        CREATE INDEX IF NOT EXISTS idx_rm_trades_state ON research_memory_trades(csi_pctile, vol_pctile, spread_pctile, tr_over_atr);
        CREATE INDEX IF NOT EXISTS idx_rm_trades_valid ON research_memory_trades(metrics_valid);
        CREATE INDEX IF NOT EXISTS idx_rm_buckets_state ON research_memory_state_buckets(state_key, bucket);
        CREATE INDEX IF NOT EXISTS idx_rm_candidates_id ON research_memory_candidates(candidate_id, run_id);
        CREATE INDEX IF NOT EXISTS idx_rm_recs_status ON research_memory_recommendations(status, recommendation_type);
        """
    )
    _add_column_if_missing(conn, "research_memory_trades", "metrics_valid", "INTEGER DEFAULT 1")
    _add_column_if_missing(conn, "research_memory_trades", "invalid_reason", "TEXT")
    for column, spec in {
        "research_tier": "TEXT",
        "research_mode": "TEXT",
        "evidence_type": "TEXT",
        "portfolio_constraints_applied": "INTEGER",
        "capital_path_valid": "INTEGER",
        "deployability_evidence": "INTEGER",
        "signal_episode_evidence": "INTEGER",
    }.items():
        _add_column_if_missing(conn, "research_memory_trades", column, spec)
    for column, spec in {
        "csi_source": "TEXT",
        "csi_components_json": "TEXT",
        "funding_raw": "REAL",
        "funding_pctile": "REAL",
        "funding_z": "REAL",
        "oi_level": "REAL",
        "oi_accel": "REAL",
        "oi_accel_pctile": "REAL",
        "oi_z": "REAL",
        "mark_price": "REAL",
        "index_price": "REAL",
        "basis_raw": "REAL",
        "basis_pct": "REAL",
        "basis_pctile": "REAL",
        "premium_pctile": "REAL",
        "crowding_proxy_pctile": "REAL",
        "constraint_stress_pctile": "REAL",
    }.items():
        _add_column_if_missing(conn, "research_memory_trades", column, spec)
    conn.commit()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, spec: str) -> None:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")
