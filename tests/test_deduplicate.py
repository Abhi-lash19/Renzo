"""
Tests for pipeline/deduplicate.py — DeduplicateEngine class and module-level shim.

Tests verify:
  - fresh engine does not mark a first job as duplicate
  - same URL seen twice is detected as a local in-memory duplicate
  - different URLs are treated as distinct jobs
  - reset() clears local state so a re-seen job is accepted again
  - a hash already stored in the repository is detected as duplicate
  - jobs with no identifying fields are safe pass-throughs (returns False)
  - the module-level is_duplicate() shim delegates correctly
  - DeduplicateEngine has a callable reset() method
"""

import sys
from datetime import datetime
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.deduplicate import DeduplicateEngine, is_duplicate
from pipeline.models import Job


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(
    url: str = "https://example.com/job/1",
    job_id: str = "test_001",
    title: str = "Python Developer",
    company: str = "TestCo",
    location: str = "Remote",
    source: str = "test_source",
) -> Job:
    return Job(
        job_id=job_id,
        title=title,
        company=company,
        location=location,
        description="python backend developer role",
        url=url,
        source=source,
        posted_at=datetime.utcnow(),
        fetched_at=datetime.utcnow(),
    )


def _mock_repo(hash_exists: bool = False) -> MagicMock:
    repo = MagicMock()
    repo.hash_exists.return_value = hash_exists
    repo.insert_hash.return_value = True
    return repo


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDeduplicateEngineFirstJob(TestCase):
    """Test 1: fresh engine — first job is never a duplicate."""

    def test_first_job_not_duplicate(self) -> None:
        engine = DeduplicateEngine()
        job = _make_job(url="https://example.com/job/42", job_id="job_42")
        repo = _mock_repo(hash_exists=False)
        result = engine.is_duplicate(job, repo)
        self.assertFalse(result)


class TestDeduplicateEngineSameUrl(TestCase):
    """Test 2: same URL seen twice triggers local in-memory duplicate detection."""

    def test_same_url_duplicate_in_memory(self) -> None:
        engine = DeduplicateEngine()
        repo = _mock_repo(hash_exists=False)

        job_a = _make_job(url="https://example.com/job/100", job_id="job_100")
        job_b = _make_job(url="https://example.com/job/100", job_id="job_100_copy")

        first = engine.is_duplicate(job_a, repo)
        second = engine.is_duplicate(job_b, repo)

        self.assertFalse(first, "First occurrence should NOT be a duplicate")
        self.assertTrue(second, "Same URL seen again should be a duplicate")


class TestDeduplicateEngineDifferentUrls(TestCase):
    """Test 3: two distinct URLs are both accepted (not duplicates)."""

    def test_different_urls_not_duplicate(self) -> None:
        engine = DeduplicateEngine()
        repo = _mock_repo(hash_exists=False)

        job_a = _make_job(url="https://example.com/job/1", job_id="job_1")
        job_b = _make_job(url="https://example.com/job/2", job_id="job_2")

        result_a = engine.is_duplicate(job_a, repo)
        result_b = engine.is_duplicate(job_b, repo)

        self.assertFalse(result_a)
        self.assertFalse(result_b)


class TestDeduplicateEngineReset(TestCase):
    """Test 4: reset() clears local state — a job seen before reset is accepted after reset."""

    def test_reset_clears_local_state(self) -> None:
        engine = DeduplicateEngine()
        repo = _mock_repo(hash_exists=False)

        job = _make_job(url="https://example.com/job/reset_test", job_id="job_reset")

        first = engine.is_duplicate(job, repo)
        self.assertFalse(first, "First occurrence should NOT be a duplicate")

        engine.reset()

        # After reset, repo must not claim the hash exists either (fresh mock)
        repo2 = _mock_repo(hash_exists=False)
        after_reset = engine.is_duplicate(job, repo2)
        self.assertFalse(after_reset, "After reset(), the same job should NOT be a duplicate")


class TestDeduplicateEngineStoredHash(TestCase):
    """Test 5: repository already has the hash — engine detects it as duplicate."""

    def test_stored_hash_is_duplicate(self) -> None:
        engine = DeduplicateEngine()
        repo = _mock_repo(hash_exists=True)  # pretend DB already has this hash

        job = _make_job(url="https://example.com/job/stored", job_id="job_stored")
        result = engine.is_duplicate(job, repo)

        self.assertTrue(result, "Job whose hash is in the repository should be a duplicate")


class TestDeduplicateEngineInvalidJob(TestCase):
    """Test 6: job with no url, no job_id, no title, no company — safe pass-through returns False."""

    def test_no_url_no_id_no_title_passes_through(self) -> None:
        engine = DeduplicateEngine()
        repo = _mock_repo(hash_exists=False)

        # Build a job then blank out the identifying fields
        job = _make_job(url="", job_id="", title="", company="")
        result = engine.is_duplicate(job, repo)

        self.assertFalse(result, "Invalid dedup input should be a safe pass-through (False)")


class TestModuleShim(TestCase):
    """Test 7: module-level is_duplicate() shim delegates to the singleton engine."""

    def test_module_shim_is_duplicate_works(self) -> None:
        import pipeline.deduplicate as dedup_mod

        # Reset the module singleton so prior test state doesn't interfere
        dedup_mod._engine.reset()

        repo = _mock_repo(hash_exists=False)
        job = _make_job(url="https://example.com/shim_test", job_id="shim_001")

        # Call the module-level function (not the class directly)
        result = is_duplicate(job, repo)

        self.assertFalse(result, "Module-level shim should return False for a fresh job")


class TestDeduplicateEngineResetMethod(TestCase):
    """Test 8: DeduplicateEngine exposes a callable reset() method."""

    def test_engine_reset_method_exists(self) -> None:
        engine = DeduplicateEngine()
        self.assertTrue(hasattr(engine, "reset"), "DeduplicateEngine must have a reset attribute")
        self.assertTrue(callable(engine.reset), "reset must be callable")
