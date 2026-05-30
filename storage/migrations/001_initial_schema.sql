-- 001_initial_schema.sql
-- Full Renzo schema for Postgres (Supabase compatible)
-- Run this on a fresh Postgres DB before any other migrations.

-- Jobs — primary data table
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    title       TEXT,
    company     TEXT,
    location    TEXT,
    description TEXT,
    url         TEXT UNIQUE,
    source      TEXT,
    posted_at   TIMESTAMPTZ,
    fetched_at  TIMESTAMPTZ,
    score       DOUBLE PRECISION,
    is_remote   BOOLEAN DEFAULT FALSE,
    is_startup  BOOLEAN DEFAULT FALSE,
    status      TEXT DEFAULT 'not_applied',
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    raw_json    TEXT,
    match_type  TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_jobs_source     ON jobs(source);
CREATE INDEX IF NOT EXISTS idx_jobs_posted_at  ON jobs(posted_at);
CREATE INDEX IF NOT EXISTS idx_jobs_score      ON jobs(score DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status     ON jobs(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_url_unique ON jobs(url);

-- Job skills (matched)
CREATE TABLE IF NOT EXISTS job_skills (
    job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    skill      TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (job_id, skill)
);

CREATE INDEX IF NOT EXISTS idx_job_skills_job_id ON job_skills(job_id);

-- Missing skills (gaps)
CREATE TABLE IF NOT EXISTS missing_skills (
    job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    skill      TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (job_id, skill)
);

CREATE INDEX IF NOT EXISTS idx_missing_skills_job_id ON missing_skills(job_id);

-- Job content hashes (deduplication)
CREATE TABLE IF NOT EXISTS job_hashes (
    hash       TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- User interactions (feedback loop)
CREATE TABLE IF NOT EXISTS user_interactions (
    id         BIGSERIAL PRIMARY KEY,
    job_id     TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    action     TEXT NOT NULL CHECK(action IN ('viewed', 'applied', 'ignored')),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_interactions_job_id     ON user_interactions(job_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_action     ON user_interactions(action);
CREATE INDEX IF NOT EXISTS idx_user_interactions_created_at ON user_interactions(created_at DESC);

-- Job runs (DB-backed pipeline queue)
CREATE TABLE IF NOT EXISTS job_runs (
    run_id       TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'queued'
                     CHECK(status IN ('queued', 'running', 'complete', 'failed')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    result_json  TEXT
);

CREATE INDEX IF NOT EXISTS idx_job_runs_status ON job_runs(status);
