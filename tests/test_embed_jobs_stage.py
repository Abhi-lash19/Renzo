import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import MagicMock, patch


class TestEmbedJobsFunction:
    def test_embed_jobs_with_no_ids_returns_0(self):
        from pipeline.orchestrator import embed_jobs
        repo = MagicMock()
        from pipeline.embedder import MockEmbeddingProvider
        count = embed_jobs([], repo, MockEmbeddingProvider())
        assert count == 0

    def test_embed_jobs_stores_embeddings_for_real_jobs(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        db_file = tmp_path / "test_embed_stage.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        from storage.db import init_db
        from storage.repository import JobRepository
        from pipeline.models import Job
        from pipeline.orchestrator import embed_jobs
        from pipeline.embedder import MockEmbeddingProvider
        from datetime import datetime

        init_db()
        repo = JobRepository()

        for i in range(3):
            job = Job(
                job_id=f"emb_test_{i:03d}", title=f"Engineer {i}", company="TestCo",
                location="Remote", description=f"Python AWS job {i}",
                url=f"https://example.com/{i}", source="test",
                posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
            )
            repo.insert_job(job)

        stored_ids = ["emb_test_000", "emb_test_001", "emb_test_002"]
        count = embed_jobs(stored_ids, repo, MockEmbeddingProvider())
        assert count == 3

        for job_id in stored_ids:
            emb = repo.get_job_embedding(job_id)
            assert emb is not None, f"No embedding for {job_id}"
            assert len(emb) == 1024

    def test_embed_jobs_returns_int(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        db_file = tmp_path / "test_embed_ret.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        from storage.db import init_db
        from storage.repository import JobRepository
        from pipeline.orchestrator import embed_jobs
        from pipeline.embedder import MockEmbeddingProvider
        init_db()
        repo = JobRepository()
        result = embed_jobs([], repo, MockEmbeddingProvider())
        assert isinstance(result, int)

    def test_embed_jobs_does_not_crash_on_provider_error(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        db_file = tmp_path / "test_embed_err.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        from storage.db import init_db
        from storage.repository import JobRepository
        from pipeline.models import Job
        from pipeline.orchestrator import embed_jobs
        from datetime import datetime
        import unittest.mock as mock

        init_db()
        repo = JobRepository()
        job = Job(
            job_id="err_job_001", title="Backend", company="Corp",
            location="Remote", description="Python",
            url="https://example.com/err", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        repo.insert_job(job)

        broken_provider = mock.MagicMock()
        broken_provider.embed_batch.side_effect = RuntimeError("model crash")
        # Should not raise — returns 0 stored
        count = embed_jobs(["err_job_001"], repo, broken_provider)
        assert count == 0
