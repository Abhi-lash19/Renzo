"""
Tests for storage.repository.JobRepository using an in-memory SQLite database.
sys.path is handled by conftest.py.
"""
import sqlite3
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from pipeline.models import Job
from storage.repository import JobRepository


@pytest.fixture
def repo_with_db():
    """Create an in-memory SQLite DB, patch db_manager to use it, return a repository."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            title TEXT, company TEXT, location TEXT, description TEXT,
            url TEXT UNIQUE, source TEXT,
            posted_at TEXT, fetched_at TEXT, score REAL DEFAULT 0.0,
            is_remote INTEGER DEFAULT 0, is_startup INTEGER DEFAULT 0,
            updated_at TEXT, match_type TEXT DEFAULT '',
            status TEXT DEFAULT 'not_applied'
        );
        CREATE TABLE job_skills (job_id TEXT, skill TEXT, created_at TEXT, PRIMARY KEY (job_id, skill));
        CREATE TABLE missing_skills (job_id TEXT, skill TEXT, created_at TEXT, PRIMARY KEY (job_id, skill));
        CREATE TABLE job_hashes (hash TEXT PRIMARY KEY, created_at TEXT);
        CREATE TABLE user_interactions (job_id TEXT, action TEXT, created_at TEXT);
    """)
    conn.commit()

    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=conn)
    mock_ctx.__exit__ = MagicMock(return_value=False)

    mock_dm = MagicMock()
    mock_dm.connection.return_value = mock_ctx

    with patch("storage.repository.db_manager", mock_dm):
        yield JobRepository()

    conn.close()


@pytest.fixture
def sample_job():
    return Job(
        job_id="test_job_001",
        title="Python Developer",
        company="TestCo",
        location="Remote",
        description="python aws backend",
        url="https://example.com/job/1",
        source="test",
        posted_at=datetime.utcnow(),
        fetched_at=datetime.utcnow(),
    )


def test_insert_job_returns_true(repo_with_db, sample_job):
    """First insert of a unique job should return True."""
    result = repo_with_db.insert_job(sample_job)
    assert result is True


def test_insert_duplicate_job_returns_false(repo_with_db, sample_job):
    """Second insert of the same URL should return False (duplicate)."""
    repo_with_db.insert_job(sample_job)
    result = repo_with_db.insert_job(sample_job)
    assert result is False


def test_insert_hash_new_returns_true(repo_with_db):
    """Inserting a new hash should return True."""
    result = repo_with_db.insert_hash("abc123hash_unique")
    assert result is True


def test_insert_hash_duplicate_returns_false(repo_with_db):
    """Inserting the same hash twice should return False on the second call."""
    repo_with_db.insert_hash("duplicate_hash_xyz")
    result = repo_with_db.insert_hash("duplicate_hash_xyz")
    assert result is False


def test_hash_exists_after_insert(repo_with_db):
    """hash_exists should return True after a hash has been inserted."""
    repo_with_db.insert_hash("known_hash_789")
    assert repo_with_db.hash_exists("known_hash_789") is True


def test_hash_not_exists_before_insert(repo_with_db):
    """hash_exists should return False for a hash that was never inserted."""
    assert repo_with_db.hash_exists("never_inserted_hash") is False


def test_insert_skills_and_retrieve(repo_with_db, sample_job):
    """After inserting a job and skills, get_job_skills should return them."""
    repo_with_db.insert_job(sample_job)
    skills = ["python", "aws", "docker"]
    repo_with_db.insert_skills(sample_job.job_id, skills)
    retrieved = repo_with_db.get_job_skills(sample_job.job_id)
    assert sorted(retrieved) == sorted(skills)


def test_record_interaction_valid_action(repo_with_db, sample_job):
    """Recording a valid action ('applied') should return True."""
    repo_with_db.insert_job(sample_job)
    result = repo_with_db.record_interaction(sample_job.job_id, "applied")
    assert result is True


def test_record_interaction_invalid_action(repo_with_db, sample_job):
    """Recording an invalid action should return False."""
    repo_with_db.insert_job(sample_job)
    result = repo_with_db.record_interaction(sample_job.job_id, "invalid")
    assert result is False


def test_get_top_jobs_returns_by_score(repo_with_db):
    """get_top_jobs should return jobs ordered by score descending."""
    low_score_job = Job(
        job_id="low_score_job",
        title="Junior Python Developer",
        company="SmallCo",
        location="Remote",
        description="python basics",
        url="https://example.com/job/low",
        source="test",
        posted_at=datetime.utcnow(),
        fetched_at=datetime.utcnow(),
    )
    low_score_job.score = 2.5

    high_score_job = Job(
        job_id="high_score_job",
        title="Senior Python Developer",
        company="BigCo",
        location="Remote",
        description="python aws kubernetes microservices",
        url="https://example.com/job/high",
        source="test",
        posted_at=datetime.utcnow(),
        fetched_at=datetime.utcnow(),
    )
    high_score_job.score = 9.0

    repo_with_db.insert_job(low_score_job)
    repo_with_db.insert_job(high_score_job)

    # Update scores in DB
    repo_with_db.update_job_score(low_score_job.job_id, 2.5)
    repo_with_db.update_job_score(high_score_job.job_id, 9.0)

    top_jobs = repo_with_db.get_top_jobs(limit=10)
    assert len(top_jobs) == 2
    assert top_jobs[0].score >= top_jobs[1].score
    assert top_jobs[0].job_id == "high_score_job"
