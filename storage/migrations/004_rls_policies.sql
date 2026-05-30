-- 004_rls_policies.sql
-- Row Level Security policies scoped by auth.uid().
-- Users can only read and write their own data.
-- Supabase service role automatically bypasses RLS.

-- Enable RLS on all user-scoped tables
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_interactions ENABLE ROW LEVEL SECURITY;

-- Drop existing policies before recreating (idempotent re-run safety)
DROP POLICY IF EXISTS "users_own_jobs" ON jobs;
DROP POLICY IF EXISTS "users_own_job_runs" ON job_runs;
DROP POLICY IF EXISTS "users_own_interactions" ON user_interactions;

-- Jobs: users see and modify only their own rows
CREATE POLICY "users_own_jobs" ON jobs
    FOR ALL
    USING (user_id = auth.uid())
    WITH CHECK (user_id = auth.uid());

-- Job runs: users see and modify only their own pipeline runs
CREATE POLICY "users_own_job_runs" ON job_runs
    FOR ALL
    USING (user_id = auth.uid())
    WITH CHECK (user_id = auth.uid());

-- Interactions: users see and modify only their own interactions
CREATE POLICY "users_own_interactions" ON user_interactions
    FOR ALL
    USING (user_id = auth.uid())
    WITH CHECK (user_id = auth.uid());

-- job_skills and missing_skills are scoped via jobs FK (no direct RLS needed)
-- job_hashes: intentionally unscoped (global deduplication across all users)
