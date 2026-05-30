-- 002_add_users.sql
-- Add user_id foreign keys to scope all data by Supabase Auth user.
-- auth.users is the Supabase Auth table (present in all Supabase projects).

-- Add user_id to jobs
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_user_id ON jobs(user_id);

-- Add user_id to job_runs
ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_job_runs_user_id ON job_runs(user_id);

-- Add user_id to user_interactions
ALTER TABLE user_interactions
    ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_user_interactions_user_id ON user_interactions(user_id);
