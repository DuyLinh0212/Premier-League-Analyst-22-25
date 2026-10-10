CREATE TABLE IF NOT EXISTS pl_analysis_runs (
    run_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    model_version TEXT,
    status TEXT NOT NULL CHECK (status IN ('processing', 'complete', 'failed')),
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
    report_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    error TEXT
);

CREATE TABLE IF NOT EXISTS pl_analysis_reports (
    run_id TEXT NOT NULL REFERENCES pl_analysis_runs(run_id),
    report_name TEXT NOT NULL,
    filter_key TEXT NOT NULL,
    filters JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload JSONB NOT NULL,
    model_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, report_name, filter_key)
);

CREATE TABLE IF NOT EXISTS pl_analysis_active_reports (
    report_name TEXT NOT NULL,
    filter_key TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES pl_analysis_runs(run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (report_name, filter_key)
);

CREATE TABLE IF NOT EXISTS pl_pipeline_jobs (
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    PRIMARY KEY (run_id, stage)
);

CREATE INDEX IF NOT EXISTS pl_analysis_reports_lookup_idx
    ON pl_analysis_reports (report_name, filter_key, created_at DESC);
CREATE INDEX IF NOT EXISTS pl_analysis_runs_created_idx
    ON pl_analysis_runs (created_at DESC);
CREATE INDEX IF NOT EXISTS pl_pipeline_jobs_started_idx
    ON pl_pipeline_jobs (started_at DESC);
