import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from storage.repository import JobRepository
from storage.db import init_db
from pipeline.embedder import MockEmbeddingProvider


@pytest.fixture
def repo(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_embeddings.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False
    init_db()
    return JobRepository()


def _sample_embedding(seed: str = "test") -> list:
    return MockEmbeddingProvider().embed(seed)


def _insert_test_job(repo: JobRepository, job_id: str = "test_001") -> bool:
    from pipeline.models import Job
    from datetime import datetime
    job = Job(
        job_id=job_id, title="Backend Engineer", company="TestCo",
        location="Remote", description="Python FastAPI",
        url=f"https://example.com/{job_id}", source="test",
        posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
    )
    return repo.insert_job(job)


class TestStoreJobEmbedding:
    def test_store_returns_true(self, repo):
        _insert_test_job(repo, "job_emb_001")
        assert repo.store_job_embedding("job_emb_001", _sample_embedding("j1")) is True

    def test_stored_embedding_retrievable(self, repo):
        _insert_test_job(repo, "job_emb_002")
        embedding = _sample_embedding("j2")
        repo.store_job_embedding("job_emb_002", embedding)
        retrieved = repo.get_job_embedding("job_emb_002")
        assert retrieved is not None
        assert len(retrieved) == 1024

    def test_stored_values_match(self, repo):
        _insert_test_job(repo, "job_emb_003")
        embedding = _sample_embedding("j3")
        repo.store_job_embedding("job_emb_003", embedding)
        retrieved = repo.get_job_embedding("job_emb_003")
        assert retrieved is not None
        assert abs(retrieved[0] - embedding[0]) < 1e-6

    def test_returns_bool(self, repo):
        _insert_test_job(repo, "job_emb_004")
        result = repo.store_job_embedding("job_emb_004", _sample_embedding())
        assert isinstance(result, bool)


class TestGetJobEmbedding:
    def test_returns_none_for_unknown_job(self, repo):
        assert repo.get_job_embedding("totally_unknown_id") is None

    def test_returns_none_before_embedding_stored(self, repo):
        _insert_test_job(repo, "job_noemb_001")
        assert repo.get_job_embedding("job_noemb_001") is None


class TestGetJobsWithoutEmbeddings:
    def test_returns_list(self, repo):
        result = repo.get_jobs_without_embeddings(limit=10)
        assert isinstance(result, list)

    def test_returns_jobs_without_embeddings(self, repo):
        _insert_test_job(repo, "job_missing_emb_001")
        missing = repo.get_jobs_without_embeddings(limit=10)
        assert "job_missing_emb_001" in missing

    def test_embedded_jobs_not_in_missing(self, repo):
        _insert_test_job(repo, "job_has_emb_001")
        repo.store_job_embedding("job_has_emb_001", _sample_embedding("emb"))
        missing = repo.get_jobs_without_embeddings(limit=10)
        assert "job_has_emb_001" not in missing

    def test_respects_limit(self, repo):
        for i in range(5):
            _insert_test_job(repo, f"job_limit_{i:03d}")
        missing = repo.get_jobs_without_embeddings(limit=3)
        assert len(missing) <= 3


class TestProfileEmbeddingStorage:
    def test_store_profile_embedding_returns_true(self, repo):
        # First create a profile row
        import json
        repo.upsert_profile("user-001", json.dumps({"core_skills": ["python"]}))
        result = repo.store_profile_embedding("user-001", _sample_embedding("profile"))
        assert result is True

    def test_get_profile_embedding_returns_vector(self, repo):
        import json
        repo.upsert_profile("user-002", json.dumps({"core_skills": ["python"]}))
        embedding = _sample_embedding("profile2")
        repo.store_profile_embedding("user-002", embedding)
        retrieved = repo.get_profile_embedding("user-002")
        assert retrieved is not None
        assert len(retrieved) == 1024

    def test_get_profile_embedding_returns_none_for_unknown(self, repo):
        assert repo.get_profile_embedding("unknown_user_xyz") is None
