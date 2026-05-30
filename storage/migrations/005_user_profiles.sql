-- 005_user_profiles.sql
-- User profile table — stores the structured profile built from resume upload.
-- user_id is nullable to allow file-based profiles for unauthenticated dev use.

CREATE TABLE IF NOT EXISTS user_profiles (
    id               TEXT PRIMARY KEY,
    user_id          UUID UNIQUE REFERENCES auth.users(id) ON DELETE CASCADE,
    name             TEXT,
    role             TEXT,
    experience_level TEXT,
    raw_text         TEXT,
    profile_json     TEXT NOT NULL,
    source           TEXT DEFAULT 'upload'
                         CHECK(source IN ('upload', 'manual', 'file')),
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_user_id ON user_profiles(user_id);

-- Auto-update updated_at on modification
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS user_profiles_updated_at ON user_profiles;
CREATE TRIGGER user_profiles_updated_at
    BEFORE UPDATE ON user_profiles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
