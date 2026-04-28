PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS hypotheses (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    layer TEXT,
    family TEXT,
    yaml_path TEXT,
    status TEXT NOT NULL,
    priority INTEGER DEFAULT 50,
    parent_hypothesis_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    notes TEXT,
    metadata_json TEXT,
    FOREIGN KEY(parent_hypothesis_id) REFERENCES hypotheses(id)
);

CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL,
    name TEXT NOT NULL,
    phase TEXT NOT NULL,
    dataset_type TEXT NOT NULL,
    experiment_root TEXT NOT NULL,
    manifest_path TEXT,
    status TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    max_workers INTEGER,
    config_path TEXT,
    local_config_path TEXT,
    data_path TEXT,
    metadata_json TEXT,
    FOREIGN KEY(hypothesis_id) REFERENCES hypotheses(id)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id TEXT PRIMARY KEY,
    hypothesis_id TEXT,
    name TEXT NOT NULL,
    phase TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    hypothesis_path TEXT NOT NULL,
    stable_experiment_id TEXT,
    volatile_experiment_id TEXT,
    verdict_bundle_path TEXT,
    log_path TEXT,
    error_message TEXT,
    commands_json TEXT,
    metadata_json TEXT,
    FOREIGN KEY(hypothesis_id) REFERENCES hypotheses(id),
    FOREIGN KEY(stable_experiment_id) REFERENCES experiments(id),
    FOREIGN KEY(volatile_experiment_id) REFERENCES experiments(id)
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    experiment_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    run_path TEXT,
    config_hash TEXT,
    status TEXT,
    ev_r_net REAL,
    ev_r_gross REAL,
    n_trades INTEGER,
    win_rate REAL,
    max_drawdown REAL,
    max_drawdown_duration REAL,
    tail_5r_count INTEGER,
    tail_10r_count INTEGER,
    avg_r_win REAL,
    avg_r_loss REAL,
    summary_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(experiment_id, run_id),
    FOREIGN KEY(experiment_id) REFERENCES experiments(id)
);

CREATE TABLE IF NOT EXISTS verdicts (
    id TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL,
    pipeline_run_id TEXT,
    verdict TEXT NOT NULL,
    confidence REAL,
    summary TEXT,
    evidence_json TEXT,
    recommended_next_action TEXT,
    next_hypothesis_id TEXT,
    memo_path TEXT,
    approved_by_user INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(hypothesis_id) REFERENCES hypotheses(id),
    FOREIGN KEY(pipeline_run_id) REFERENCES pipeline_runs(id),
    FOREIGN KEY(next_hypothesis_id) REFERENCES hypotheses(id)
);

CREATE TABLE IF NOT EXISTS queues (
    id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    item_type TEXT NOT NULL,
    item_id TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER DEFAULT 50,
    payload_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    available_after TEXT,
    locked_at TEXT,
    locked_by TEXT,
    attempts INTEGER DEFAULT 0,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS state_findings (
    id TEXT PRIMARY KEY,
    hypothesis_id TEXT,
    experiment_id TEXT,
    state_variable TEXT NOT NULL,
    bucket TEXT NOT NULL,
    dataset_type TEXT,
    n_trades INTEGER,
    ev_r_net REAL,
    median_r REAL,
    p95_r REAL,
    p99_r REAL,
    max_r REAL,
    min_r REAL,
    notes TEXT,
    evidence_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(hypothesis_id) REFERENCES hypotheses(id),
    FOREIGN KEY(experiment_id) REFERENCES experiments(id)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    hypothesis_id TEXT,
    experiment_id TEXT,
    pipeline_run_id TEXT,
    artifact_type TEXT NOT NULL,
    path TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL,
    metadata_json TEXT,
    FOREIGN KEY(hypothesis_id) REFERENCES hypotheses(id),
    FOREIGN KEY(experiment_id) REFERENCES experiments(id),
    FOREIGN KEY(pipeline_run_id) REFERENCES pipeline_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_hypotheses_status ON hypotheses(status);
CREATE INDEX IF NOT EXISTS idx_hypotheses_priority ON hypotheses(priority);
CREATE INDEX IF NOT EXISTS idx_experiments_hypothesis_id ON experiments(hypothesis_id);
CREATE INDEX IF NOT EXISTS idx_experiments_status ON experiments(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_experiment_id ON runs(experiment_id);
CREATE INDEX IF NOT EXISTS idx_verdicts_hypothesis_id ON verdicts(hypothesis_id);
CREATE INDEX IF NOT EXISTS idx_queues_name_status_priority ON queues(queue_name, status, priority);
CREATE INDEX IF NOT EXISTS idx_state_findings_state_variable ON state_findings(state_variable);
CREATE INDEX IF NOT EXISTS idx_artifacts_hypothesis_id ON artifacts(hypothesis_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_experiment_id ON artifacts(experiment_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_pipeline_run_id ON artifacts(pipeline_run_id);
