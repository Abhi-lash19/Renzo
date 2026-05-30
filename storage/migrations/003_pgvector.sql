-- 003_pgvector.sql
-- Enable pgvector extension and add embedding column to jobs.
-- Requires pgvector extension (available in Supabase by default).

CREATE EXTENSION IF NOT EXISTS vector;

-- Add 1024-dimensional embedding column (BAAI/bge-large-en-v1.5)
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS embedding VECTOR(1024);

-- IVFFlat index for approximate nearest-neighbor search
-- lists=100 is appropriate for up to ~1M rows; tune after Phase 5 benchmarks
CREATE INDEX IF NOT EXISTS idx_jobs_embedding_ivfflat
    ON jobs USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
