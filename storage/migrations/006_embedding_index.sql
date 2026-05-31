-- 006_embedding_index.sql
-- Tune the pgvector index on jobs.embedding for cosine distance.
-- jobs.embedding VECTOR(1024) was created in migration 003.
-- Also adds profile_embedding VECTOR(1024) to user_profiles.

-- Re-create index with explicit cosine distance operator class
DROP INDEX IF EXISTS idx_jobs_embedding_ivfflat;
CREATE INDEX IF NOT EXISTS idx_jobs_embedding_cosine
    ON jobs USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Profile embedding column for Postgres
ALTER TABLE user_profiles
    ADD COLUMN IF NOT EXISTS profile_embedding VECTOR(1024);
