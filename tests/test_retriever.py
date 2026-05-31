import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import math
from pipeline.retriever import (
    cosine_similarity,
    fuse_scores,
    retrieve_keyword_only,
    retrieve_hybrid,
    RetrievalResult,
)


class TestCosineSimilarity:
    def test_identical_vectors_returns_1(self):
        v = [1.0, 0.0, 0.0]
        assert abs(cosine_similarity(v, v) - 1.0) < 1e-6

    def test_orthogonal_vectors_returns_0(self):
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        assert abs(cosine_similarity(a, b)) < 1e-6

    def test_opposite_vectors_returns_negative_1(self):
        a = [1.0, 0.0]
        b = [-1.0, 0.0]
        assert abs(cosine_similarity(a, b) - (-1.0)) < 1e-6

    def test_returns_float(self):
        result = cosine_similarity([0.5, 0.5], [0.5, 0.5])
        assert isinstance(result, float)

    def test_zero_vector_returns_0(self):
        assert cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0

    def test_result_in_valid_range(self):
        a = [1.0 / math.sqrt(3)] * 3
        b = [1.0 / math.sqrt(3)] * 3
        sim = cosine_similarity(a, b)
        assert -1.0 <= sim <= 1.0 + 1e-6

    def test_partial_overlap_between_0_and_1(self):
        a = [1.0, 1.0, 0.0]
        b = [1.0, 0.0, 1.0]
        sim = cosine_similarity(a, b)
        assert 0.0 < sim < 1.0

    def test_empty_vectors_return_0(self):
        assert cosine_similarity([], []) == 0.0


class TestFuseScores:
    def test_returns_float(self):
        result = fuse_scores(keyword_score=7.5, vector_score=0.8)
        assert isinstance(result, float)

    def test_max_scores_gives_1(self):
        result = fuse_scores(keyword_score=10.0, vector_score=1.0, kw_weight=0.6, vec_weight=0.4)
        assert abs(result - 1.0) < 1e-6

    def test_zero_scores_gives_0(self):
        result = fuse_scores(keyword_score=0.0, vector_score=0.0)
        assert abs(result) < 1e-6

    def test_custom_weights_kw_only(self):
        result = fuse_scores(keyword_score=8.0, vector_score=0.0, kw_weight=1.0, vec_weight=0.0)
        assert abs(result - 0.8) < 1e-6

    def test_result_in_0_to_1_range(self):
        result = fuse_scores(keyword_score=6.5, vector_score=0.7)
        assert 0.0 <= result <= 1.0

    def test_known_computation(self):
        # 0.6 * (8/10) + 0.4 * 0.9 = 0.48 + 0.36 = 0.84
        result = fuse_scores(keyword_score=8.0, vector_score=0.9, kw_weight=0.6, vec_weight=0.4)
        assert abs(result - 0.84) < 1e-6

    def test_higher_vector_increases_fused(self):
        low = fuse_scores(keyword_score=5.0, vector_score=0.2)
        high = fuse_scores(keyword_score=5.0, vector_score=0.9)
        assert high > low

    def test_negative_vector_score_clamped_to_0(self):
        result = fuse_scores(keyword_score=5.0, vector_score=-0.5)
        assert result >= 0.0


class TestRetrievalResult:
    def test_is_dataclass(self):
        from dataclasses import is_dataclass
        assert is_dataclass(RetrievalResult)

    def test_has_required_fields(self):
        result = RetrievalResult(
            job_id="j1", keyword_score=7.0, vector_score=0.8, fused_score=0.74
        )
        assert result.job_id == "j1"
        assert result.fused_score == 0.74
        assert result.vector_score == 0.8

    def test_fused_score_is_float(self):
        result = RetrievalResult(job_id="j2", keyword_score=5.0, vector_score=0.5, fused_score=0.5)
        assert isinstance(result.fused_score, float)


class TestRetrieveKeywordOnly:
    def test_returns_list(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        db_file = tmp_path / "test_retriever_kw.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        from storage.db import init_db
        from storage.repository import JobRepository
        init_db()
        repo = JobRepository()
        result = retrieve_keyword_only(repo, limit=10)
        assert isinstance(result, list)

    def test_empty_db_returns_empty(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        db_file = tmp_path / "test_retriever_empty.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        from storage.db import init_db
        from storage.repository import JobRepository
        init_db()
        repo = JobRepository()
        assert retrieve_keyword_only(repo, limit=10) == []


class TestRetrieveHybridSQLite:
    """End-to-end test of the SQLite Python-side cosine path in retrieve_hybrid()."""

    @pytest.fixture
    def repo_with_embedded_jobs(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        db_file = tmp_path / "test_hybrid_retrieval.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        from storage.db import init_db
        from storage.repository import JobRepository
        from pipeline.models import Job
        from pipeline.embedder import MockEmbeddingProvider
        from datetime import datetime

        init_db()
        repo = JobRepository()
        provider = MockEmbeddingProvider()

        jobs_data = [
            ("job_hyb_001", "Python Backend Engineer", "TechCo", "Python FastAPI AWS Docker"),
            ("job_hyb_002", "Frontend React Developer", "WebCo", "React JavaScript CSS UI"),
            ("job_hyb_003", "DevOps Engineer", "CloudCo", "Kubernetes Terraform AWS Docker"),
        ]
        for job_id, title, company, description in jobs_data:
            job = Job(
                job_id=job_id, title=title, company=company,
                location="Remote", description=description,
                url=f"https://example.com/{job_id}", source="test",
                posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
            )
            job.score = 7.0 if "Python" in description else 5.0
            repo.insert_job(job)
            # Store embedding for this job
            job_text = f"{title} at {company}. {description}"
            emb = provider.embed(job_text)
            repo.store_job_embedding(job_id, emb)

        return repo, provider

    def test_retrieve_hybrid_returns_list(self, repo_with_embedded_jobs):
        repo, provider = repo_with_embedded_jobs
        profile_emb = provider.embed("Python developer AWS backend FastAPI")
        results = retrieve_hybrid(profile_emb, repo, limit=10)
        assert isinstance(results, list)

    def test_retrieve_hybrid_returns_retrieval_results(self, repo_with_embedded_jobs):
        from pipeline.retriever import RetrievalResult
        repo, provider = repo_with_embedded_jobs
        profile_emb = provider.embed("Python developer AWS backend FastAPI")
        results = retrieve_hybrid(profile_emb, repo, limit=10)
        assert len(results) > 0
        assert all(isinstance(r, RetrievalResult) for r in results)

    def test_retrieve_hybrid_returns_nonzero_fused_scores(self, repo_with_embedded_jobs):
        repo, provider = repo_with_embedded_jobs
        profile_emb = provider.embed("Python developer AWS backend FastAPI")
        results = retrieve_hybrid(profile_emb, repo, limit=10)
        assert all(r.fused_score > 0.0 for r in results)

    def test_retrieve_hybrid_fused_scores_in_range(self, repo_with_embedded_jobs):
        repo, provider = repo_with_embedded_jobs
        profile_emb = provider.embed("Python developer AWS backend FastAPI")
        results = retrieve_hybrid(profile_emb, repo, limit=10)
        for r in results:
            assert 0.0 <= r.fused_score <= 1.0, f"Out of range: {r.job_id} fused={r.fused_score}"

    def test_retrieve_hybrid_sorted_by_fused_score(self, repo_with_embedded_jobs):
        repo, provider = repo_with_embedded_jobs
        profile_emb = provider.embed("Python developer AWS backend FastAPI")
        results = retrieve_hybrid(profile_emb, repo, limit=10)
        scores = [r.fused_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_retrieve_hybrid_empty_profile_fallback(self, repo_with_embedded_jobs):
        repo, _ = repo_with_embedded_jobs
        # Empty profile embedding → fallback to keyword-only
        results = retrieve_hybrid([], repo, limit=10)
        assert isinstance(results, list)
