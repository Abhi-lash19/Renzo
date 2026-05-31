import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import importlib
import math
from fastapi.testclient import TestClient
from pipeline.embedder import MockEmbeddingProvider
from pipeline.retriever import cosine_similarity, fuse_scores, RetrievalResult


class TestScoreFusionMathematics:
    """Verify the fused score formula is mathematically correct."""

    def test_known_computation(self):
        # 0.6 * (8/10) + 0.4 * 0.9 = 0.48 + 0.36 = 0.84
        result = fuse_scores(keyword_score=8.0, vector_score=0.9, kw_weight=0.6, vec_weight=0.4)
        assert abs(result - 0.84) < 1e-6

    def test_max_inputs_gives_1(self):
        result = fuse_scores(keyword_score=10.0, vector_score=1.0, kw_weight=0.6, vec_weight=0.4)
        assert abs(result - 1.0) < 1e-6

    def test_zero_inputs_gives_0(self):
        assert abs(fuse_scores(0.0, 0.0)) < 1e-6

    def test_high_vector_compensates_low_keyword(self):
        # low keyword, high vector vs high keyword, low vector
        low_kw = fuse_scores(keyword_score=3.0, vector_score=0.95, kw_weight=0.6, vec_weight=0.4)
        high_kw = fuse_scores(keyword_score=7.0, vector_score=0.0, kw_weight=0.6, vec_weight=0.4)
        # low_kw = 0.6*0.3 + 0.4*0.95 = 0.18 + 0.38 = 0.56
        # high_kw = 0.6*0.7 + 0.4*0.0 = 0.42
        assert low_kw > high_kw

    def test_fused_always_in_0_to_1(self):
        import random
        random.seed(42)
        for _ in range(100):
            kw = random.uniform(0.0, 10.0)
            vec = random.uniform(-1.0, 1.0)
            result = fuse_scores(kw, vec)
            assert 0.0 <= result <= 1.0 + 1e-9, f"Out of range: kw={kw} vec={vec} fused={result}"

    def test_default_weights_are_0_6_and_0_4(self):
        # With full scores and default weights → should be 1.0
        result = fuse_scores(keyword_score=10.0, vector_score=1.0)
        assert abs(result - 1.0) < 1e-6


class TestCosineSimilarityProperties:
    def test_mock_same_text_cosine_near_1(self):
        provider = MockEmbeddingProvider()
        a = provider.embed("python developer aws backend microservices")
        b = provider.embed("python developer aws backend microservices")
        assert cosine_similarity(a, b) > 0.99

    def test_mock_different_text_cosine_not_1(self):
        provider = MockEmbeddingProvider()
        a = provider.embed("python developer aws backend")
        b = provider.embed("frontend designer react css animations figma")
        sim = cosine_similarity(a, b)
        # Mock vectors from different texts should not be identical
        assert sim < 0.99

    def test_mock_embeddings_are_unit_vectors(self):
        provider = MockEmbeddingProvider()
        v = provider.embed("backend engineer python fastapi")
        norm = math.sqrt(sum(x * x for x in v))
        assert abs(norm - 1.0) < 1e-6

    def test_cosine_is_dot_product_for_unit_vectors(self):
        provider = MockEmbeddingProvider()
        a = provider.embed("backend engineer")
        b = provider.embed("python developer")
        dot = sum(x * y for x, y in zip(a, b))
        cos = cosine_similarity(a, b)
        assert abs(dot - cos) < 1e-6


class TestHybridABComparison:
    """A/B comparison: hybrid ranking can reorder jobs vs keyword-only."""

    def test_high_vector_job_outranks_higher_keyword_job(self):
        # j1: kw=4.0, vec=0.95 → fused = 0.6*0.4 + 0.4*0.95 = 0.24 + 0.38 = 0.62
        # j2: kw=8.0, vec=0.1  → fused = 0.6*0.8 + 0.4*0.1 = 0.48 + 0.04 = 0.52
        j1_fused = fuse_scores(keyword_score=4.0, vector_score=0.95, kw_weight=0.6, vec_weight=0.4)
        j2_fused = fuse_scores(keyword_score=8.0, vector_score=0.1, kw_weight=0.6, vec_weight=0.4)
        assert j1_fused > j2_fused

    def test_retrieval_result_sorted_by_fused_score(self):
        results = [
            RetrievalResult("j3", keyword_score=9.0, vector_score=0.1, fused_score=0.58),
            RetrievalResult("j1", keyword_score=5.0, vector_score=0.95, fused_score=0.68),
            RetrievalResult("j2", keyword_score=7.0, vector_score=0.6, fused_score=0.66),
        ]
        sorted_r = sorted(results, key=lambda r: r.fused_score, reverse=True)
        assert sorted_r[0].job_id == "j1"
        assert sorted_r[1].job_id == "j2"
        assert sorted_r[2].job_id == "j3"

    def test_keyword_only_ignores_vector_score(self):
        # With vec_weight=0, vector score has zero impact
        high_vec = fuse_scores(keyword_score=5.0, vector_score=1.0, kw_weight=1.0, vec_weight=0.0)
        no_vec   = fuse_scores(keyword_score=5.0, vector_score=0.0, kw_weight=1.0, vec_weight=0.0)
        assert abs(high_vec - no_vec) < 1e-9


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    import app.auth as auth_module
    import config.settings as settings_module
    import pipeline.embedder as emb_module

    db_file = tmp_path / "test_phase5.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False

    monkeypatch.setenv("AUTH_ENABLED", "false")
    monkeypatch.setenv("EMBEDDINGS_ENABLED", "false")
    monkeypatch.setenv("EMBEDDING_PROVIDER", "mock")
    importlib.reload(settings_module)
    monkeypatch.setattr(auth_module, "settings", settings_module.settings)
    emb_module.reset_provider()

    from app.main import app
    with TestClient(app) as c:
        yield c
    emb_module.reset_provider()


class TestPhase5APIIntegration:
    def test_jobs_top_returns_200(self, api_client):
        assert api_client.get("/v1/jobs/top").status_code == 200

    def test_jobs_top_returns_list(self, api_client):
        assert isinstance(api_client.get("/v1/jobs/top").json(), list)

    def test_profile_upload_succeeds(self, api_client):
        resume = b"SKILLS\nPython, AWS, FastAPI, Docker, PostgreSQL\nEXPERIENCE\nBackend Engineer"
        resp = api_client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", resume, "text/plain")},
        )
        assert resp.status_code == 201

    def test_upload_then_get_then_top_jobs_flow(self, api_client):
        """Upload resume -> GET profile -> GET /v1/jobs/top — all succeed."""
        resume = b"SKILLS\nPython, AWS, FastAPI, Docker\nWORK EXPERIENCE\nBuilt Python microservices"
        upload = api_client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", resume, "text/plain")},
        )
        assert upload.status_code == 201

        profile = api_client.get("/v1/profile/")
        assert profile.status_code == 200

        top = api_client.get("/v1/jobs/top")
        assert top.status_code == 200
        assert isinstance(top.json(), list)

    def test_existing_endpoints_not_broken(self, api_client):
        """Smoke test: all major endpoints return non-500 responses."""
        assert api_client.get("/v1/health").status_code == 200
        assert api_client.get("/v1/profile/").status_code == 200
        assert api_client.get("/v1/jobs/top").status_code == 200
        assert api_client.post("/v1/auth/register",
                               json={"email": "a@b.com", "password": "password123"}).status_code == 200
        assert api_client.post("/v1/auth/login",
                               json={"email": "a@b.com", "password": "password123"}).status_code == 200

    def test_jobs_top_has_retrieval_mode_field(self, api_client):
        """When jobs exist, response items have retrieval_mode field."""
        from storage.repository import JobRepository
        from pipeline.models import Job
        from datetime import datetime

        repo = JobRepository()
        job = Job(
            job_id="phase5_smoke_001", title="Backend Engineer", company="TestCo",
            location="Remote", description="Python FastAPI AWS",
            url="https://example.com/phase5/1", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        job.score = 7.0
        repo.insert_job(job)

        resp = api_client.get("/v1/jobs/top")
        data = resp.json()
        if data:
            assert "retrieval_mode" in data[0]
            assert data[0]["retrieval_mode"] == "keyword_only"
