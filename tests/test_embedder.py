import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from pipeline.embedder import (
    MockEmbeddingProvider,
    build_job_text,
    build_profile_text,
    embed_text,
    get_embedding_provider,
    reset_provider,
)
from pipeline.models import Job
from datetime import datetime


def _make_job(**kwargs) -> Job:
    defaults = dict(
        job_id="test_001", title="Backend Engineer", company="TestCo",
        location="Remote", description="Python FastAPI AWS Lambda microservices",
        url="https://example.com/1", source="test",
        posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
    )
    defaults.update(kwargs)
    return Job(**defaults)


def _make_profile(**kwargs) -> dict:
    defaults = dict(
        all_skills=["python", "aws", "fastapi", "docker"],
        experience=["backend development", "cloud automation"],
        role="Backend Developer",
    )
    defaults.update(kwargs)
    return defaults


class TestMockEmbeddingProvider:
    def test_embed_returns_list_of_floats(self):
        provider = MockEmbeddingProvider()
        result = provider.embed("hello world")
        assert isinstance(result, list)
        assert all(isinstance(x, float) for x in result)

    def test_embed_returns_correct_dimensions(self):
        provider = MockEmbeddingProvider()
        result = provider.embed("hello world")
        assert len(result) == 1024

    def test_embed_is_deterministic(self):
        provider = MockEmbeddingProvider()
        a = provider.embed("same text")
        b = provider.embed("same text")
        assert a == b

    def test_embed_differs_for_different_text(self):
        provider = MockEmbeddingProvider()
        a = provider.embed("python developer")
        b = provider.embed("frontend designer")
        assert a != b

    def test_embed_batch_returns_correct_count(self):
        provider = MockEmbeddingProvider()
        results = provider.embed_batch(["text1", "text2", "text3"])
        assert len(results) == 3

    def test_embed_batch_each_has_correct_dims(self):
        provider = MockEmbeddingProvider()
        results = provider.embed_batch(["hello", "world"])
        assert all(len(r) == 1024 for r in results)

    def test_dimensions_property(self):
        provider = MockEmbeddingProvider()
        assert provider.dimensions == 1024

    def test_empty_text_returns_zero_vector(self):
        provider = MockEmbeddingProvider()
        result = provider.embed("")
        assert len(result) == 1024
        assert all(x == 0.0 for x in result)


class TestBuildJobText:
    def test_includes_title(self):
        job = _make_job(title="Python Developer")
        text = build_job_text(job)
        assert "Python Developer" in text

    def test_includes_company(self):
        job = _make_job(company="AwesomeCorp")
        text = build_job_text(job)
        assert "AwesomeCorp" in text

    def test_includes_description(self):
        job = _make_job(description="FastAPI and PostgreSQL")
        text = build_job_text(job)
        assert "FastAPI" in text

    def test_truncates_long_description(self):
        long_desc = "x" * 3000
        job = _make_job(description=long_desc)
        text = build_job_text(job)
        assert len(text) < 3000

    def test_returns_string(self):
        assert isinstance(build_job_text(_make_job()), str)


class TestBuildProfileText:
    def test_includes_skills(self):
        profile = _make_profile(all_skills=["python", "aws", "fastapi"])
        text = build_profile_text(profile)
        assert "python" in text.lower()

    def test_includes_role(self):
        profile = _make_profile(role="Backend Developer")
        text = build_profile_text(profile)
        assert "Backend" in text

    def test_returns_string(self):
        assert isinstance(build_profile_text(_make_profile()), str)

    def test_empty_profile_returns_string(self):
        assert isinstance(build_profile_text({}), str)


class TestEmbedText:
    def test_returns_list(self):
        provider = MockEmbeddingProvider()
        result = embed_text("test text", provider)
        assert isinstance(result, list)

    def test_has_correct_dimensions(self):
        provider = MockEmbeddingProvider()
        result = embed_text("test text", provider)
        assert len(result) == 1024


class TestGetEmbeddingProvider:
    def test_returns_mock_when_configured(self, monkeypatch):
        import config.settings as s
        reset_provider()
        monkeypatch.setattr(s.settings, "EMBEDDING_PROVIDER", "mock")
        provider = get_embedding_provider()
        assert isinstance(provider, MockEmbeddingProvider)
        reset_provider()

    def test_is_singleton(self, monkeypatch):
        import config.settings as s
        reset_provider()
        monkeypatch.setattr(s.settings, "EMBEDDING_PROVIDER", "mock")
        p1 = get_embedding_provider()
        p2 = get_embedding_provider()
        assert p1 is p2
        reset_provider()
