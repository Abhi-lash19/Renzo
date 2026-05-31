"""
Pipeline orchestrator — coordinates all processing stages for a batch of jobs.

Stages in order:
  1. load_learning_preferences  — inject interaction history into the profile
  2. prepare_jobs_with_match_data — finalise job_id, build match_data
  3. filter_jobs                 — relevance + age filter
  4. deduplicate_jobs            — URL / id / fuzzy hash dedup
  5. enrich_jobs                 — score each job
  6. store_jobs                  — persist to DB
  7. generate_intelligence       — missing skills, insights, score update
  8. refresh_learning_preferences — update learned state after the run

Entry point: process_jobs(jobs, repository, profile) -> int (count stored)
"""

import time
import types
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

from config.settings import settings
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.remotive_api import RemotiveFetcher
from intelligence.feedback_loop import attach_user_preferences, get_user_preferences
from intelligence.resume_enhancer import generate_insight
from intelligence.skill_gap import compute_skill_gap
from pipeline.classifier import classify_job
from pipeline.deduplicate import is_duplicate
from pipeline.filter import passes_filter
from pipeline.models import Job
from pipeline.scorer import score_job
from storage.repository import JobRepository
from utils.logger import get_logger
from utils.matching_engine import build_match_data
from utils.validation import validate_job
from core.exceptions import RenzoValidationError

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stage_log(stage: str, started_at: float, message: str) -> None:
    elapsed = time.perf_counter() - started_at
    logger.info(f"[{stage}] {message} | {elapsed:.2f}s")


def _finalize_job_id(job: Job) -> None:
    """
    Apply the source prefix to job_id once, before any pipeline processing.

    Rationale: the raw job_id from each fetcher (e.g. "12345" from Adzuna) is
    only unique within that source. Prefixing with source name makes it globally
    unique for DB storage and log traceability. The prefix is applied here — the
    earliest pipeline stage — so every subsequent log entry, dedup hash, and DB
    write uses the same final identifier.

    Guard: if the id already starts with "{source}_" (e.g. job re-enters the
    pipeline) the prefix is not applied twice.
    """
    if job.job_id and not job.job_id.startswith(f"{job.source}_"):
        job.job_id = f"{job.source}_{job.job_id}"


# ---------------------------------------------------------------------------
# Stage functions
# ---------------------------------------------------------------------------

def load_learning_preferences(repository: JobRepository, profile: dict) -> dict:
    preferences = get_user_preferences(repository, profile)
    attach_user_preferences(profile, preferences)
    logger.info(
        f"[LEARNING_LOAD] applied={preferences.get('applied_jobs_count', 0)} "
        f"ignored={preferences.get('ignored_jobs_count', 0)} "
        f"preferred_skills={preferences.get('preferred_skills', [])[:5]}"
    )
    return preferences


def refresh_learning_preferences(repository: JobRepository, profile: dict) -> dict:
    preferences = get_user_preferences(repository, profile)
    attach_user_preferences(profile, preferences)
    logger.info(
        f"[LEARNING_REFRESH] applied={preferences.get('applied_jobs_count', 0)} "
        f"ignored={preferences.get('ignored_jobs_count', 0)} "
        f"preferred_roles={preferences.get('preferred_roles', [])[:5]}"
    )
    return preferences


def prepare_jobs_with_match_data(jobs: List[Job], profile: dict) -> List[Job]:
    """
    Finalise each job_id (source prefix) then build match_data.

    Applying the prefix here — before filter, dedup, and scoring — ensures the
    same identifier is used throughout all pipeline stages and in every log line.
    """
    prepared_jobs: List[Job] = []
    for job in jobs[: settings.JOB_FETCH_LIMIT]:
        try:
            _finalize_job_id(job)
            try:
                validate_job(job)
            except RenzoValidationError as e:
                logger.warning(
                    f"[PREPARE] Skipping invalid job job_id={getattr(job, 'job_id', 'unknown')}: {e}"
                )
                continue
            match_data = build_match_data(job, profile)
            if not match_data:
                raise ValueError("match_data must be built before filtering")
            prepared_jobs.append(job)
        except Exception as error:
            logger.exception(
                f"[PIPELINE_ERROR] Failed to prepare job "
                f"job_id={getattr(job, 'job_id', 'unknown')} "
                f"title={getattr(job, 'title', '')}: {error}"
            )
    return prepared_jobs


def filter_jobs(jobs: List[Job], profile: dict, fallback: bool = False) -> Tuple[List[Job], int, float]:
    accepted_jobs: List[Job] = []
    filtered_count = 0
    total_score = 0.0

    threshold = 3 if fallback else 4
    limit_jobs = jobs

    for job in limit_jobs:
        try:
            passed, reason, filter_score = passes_filter(job, profile, threshold=threshold)
            total_score += filter_score
            if passed:
                accepted_jobs.append(job)
            else:
                filtered_count += 1
        except Exception as e:
            logger.exception(f"Error filtering job: {e}")
            filtered_count += 1

    # Fallback: relax skill/role threshold when very few jobs pass.
    # The age filter inside passes_filter() is never relaxed.
    if not fallback and len(accepted_jobs) < 20:
        logger.info(f"Only {len(accepted_jobs)} passed. Engaging fallback threshold=3")
        accepted_jobs = []
        filtered_count = 0
        total_score = 0.0
        for job in limit_jobs:
            try:
                passed, reason, filter_score = passes_filter(job, profile, threshold=3)
                total_score += filter_score
                if passed:
                    accepted_jobs.append(job)
                else:
                    filtered_count += 1
            except Exception:
                filtered_count += 1

    evaluated_count = len(limit_jobs)
    avg_score = (total_score / evaluated_count) if evaluated_count > 0 else 0.0
    return accepted_jobs, filtered_count, avg_score


def deduplicate_jobs(jobs: List[Job], repository: JobRepository) -> Tuple[List[Job], int]:
    unique_jobs: List[Job] = []
    duplicate_count = 0

    # Reset per-run in-memory state via the engine's public API.
    import pipeline.deduplicate
    pipeline.deduplicate._engine.reset()

    for job in jobs:
        try:
            if is_duplicate(job, repository):
                duplicate_count += 1
            else:
                unique_jobs.append(job)
        except Exception as e:
            logger.exception(f"Error deduplicating job: {e}")
            unique_jobs.append(job)
    return unique_jobs, duplicate_count


def enrich_jobs(jobs: List[Job], profile: dict) -> Tuple[List[Job], float]:
    """
    Score each job using the weighted formula.

    match_data must already be set on every job before this stage runs.
    Skill extraction is not performed here — build_match_data() is the
    single source of truth and has already been called.
    """
    enriched_jobs: List[Job] = []
    total_score = 0.0

    for job in jobs:
        try:
            if not getattr(job, "match_data", None):
                logger.error(
                    f"[PIPELINE_ERROR] Missing match_data before scoring "
                    f"job_id={getattr(job, 'job_id', 'unknown')} title={getattr(job, 'title', '')}"
                )
                raise ValueError("match_data must be built before scoring")

            logger.info(
                f"[SCORER_INPUT] job_id={getattr(job, 'job_id', 'unknown')} "
                f"match_data={getattr(job, 'match_data', {})}"
            )

            score = score_job(job, profile)
            job.match_type = classify_job(score=job.score, transferable_count=0)
            logger.info(
                f"[SCORER] job_id={getattr(job, 'job_id', 'unknown')} "
                f"title={getattr(job, 'title', '')} "
                f"score={score} "
                f"match_type={job.match_type} "
                f"breakdown={getattr(job, 'score_breakdown', {})}"
            )
            total_score += score
            enriched_jobs.append(job)

        except Exception as e:
            logger.exception(
                f"Error enriching job job_id={getattr(job, 'job_id', 'unknown')} "
                f"title={getattr(job, 'title', '')}: {e}"
            )

    avg_score = (total_score / len(enriched_jobs)) if enriched_jobs else 0.0
    return enriched_jobs, avg_score


def store_jobs(jobs: List[Job], repository: JobRepository) -> List[Job]:
    """
    Persist scored jobs to the database.

    job_id is already finalised (source-prefixed) by prepare_jobs_with_match_data();
    this function does not mutate it.
    """
    stored_jobs: List[Job] = []
    for job in jobs:
        try:
            if not job.job_id:
                logger.warning(f"Skipping job with missing ID: {job.title}")
                continue
            if not getattr(job, "match_data", None):
                raise ValueError("match_data must exist before storing")

            if repository.insert_job(job):
                repository.insert_skills(job.job_id, job.skills)
                stored_jobs.append(job)
        except Exception as e:
            logger.exception(f"Storage failed for job: {e}")
    return stored_jobs


def generate_intelligence(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    """
    Compute skill gaps, generate insights, and persist missing skills + updated score.

    Skills were already written by store_jobs(); this stage writes only missing_skills
    and updates the score. Calling insert_skills() again here was a duplicate write
    that has been removed.
    """
    intelligence_count = 0
    for job in jobs:
        try:
            if not getattr(job, "match_data", None):
                raise ValueError("match_data must exist before intelligence generation")
            compute_skill_gap(job, profile)
            job.insight = generate_insight(job, profile)
            missing_saved = repository.insert_missing_skills(job.job_id, job.missing_skills)
            score_saved = repository.update_job_score(job.job_id, job.score)
            if missing_saved and score_saved:
                intelligence_count += 1
        except Exception as e:
            logger.exception(
                f"Error generating intelligence for job "
                f"{getattr(job, 'job_id', 'unknown')}: {e}"
            )
    return intelligence_count


# ---------------------------------------------------------------------------
# Phase 5: Embedding stage
# ---------------------------------------------------------------------------

def _fetch_jobs_for_embedding(job_ids: List[str]) -> Dict[str, str]:
    """
    Fetch title + company + description for the given job_ids.
    Returns {job_id: embedding_text} for use by embed_jobs().
    """
    if not job_ids:
        return {}

    from pipeline.embedder import build_job_text
    from storage.db_manager import db_manager

    placeholders = ",".join("?" for _ in job_ids)
    query = f"SELECT id, title, company, description FROM jobs WHERE id IN ({placeholders})"

    try:
        with db_manager.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(job_ids))
            rows = cursor.fetchall()
    except Exception as e:
        logger.error(f"[EMBED_STAGE] Failed to fetch job data for embedding: {e}")
        return {}

    result: Dict[str, str] = {}
    for row in rows:
        job_id, title, company, description = row

        proxy = types.SimpleNamespace(
            title=title or "",
            company=company or "",
            description=description or "",
        )
        result[job_id] = build_job_text(proxy)

    return result


def embed_jobs(
    job_ids: List[str],
    repository: JobRepository,
    provider,
    batch_size: int | None = None,
) -> int:
    """
    Generate and store embeddings for the given job_ids.
    Called after store_jobs() when EMBEDDINGS_ENABLED=true.

    Returns the count of successfully stored embeddings.
    Failures are logged but never propagated — the stage is non-fatal.
    """
    if not job_ids:
        return 0

    _batch_size = batch_size or settings.EMBEDDING_BATCH_SIZE
    job_texts = _fetch_jobs_for_embedding(job_ids)
    if not job_texts:
        logger.warning("[EMBED_STAGE] No job texts fetched for embedding")
        return 0

    ids_ordered = list(job_texts.keys())
    texts_ordered = [job_texts[jid] for jid in ids_ordered]
    stored_count = 0

    for batch_start in range(0, len(ids_ordered), _batch_size):
        batch_ids = ids_ordered[batch_start: batch_start + _batch_size]
        batch_texts = texts_ordered[batch_start: batch_start + _batch_size]
        try:
            embeddings = provider.embed_batch(batch_texts)
        except Exception as e:
            logger.error(
                f"[EMBED_STAGE] embed_batch failed at offset {batch_start}: {e}",
                extra={"component": "EMBED_STAGE", "event": "batch_error",
                       "meta": {"offset": batch_start, "error": str(e)}}
            )
            continue
        for job_id, embedding in zip(batch_ids, embeddings):
            if repository.store_job_embedding(job_id, embedding):
                stored_count += 1

    logger.info(
        f"[EMBED_STAGE] Embedded {stored_count}/{len(job_ids)} jobs",
        extra={"component": "EMBED_STAGE", "event": "embed_complete",
               "meta": {"total": len(job_ids), "stored": stored_count}}
    )
    return stored_count


# ---------------------------------------------------------------------------
# Orchestrator entry point
# ---------------------------------------------------------------------------

def process_jobs(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    """
    Run the full processing pipeline for a batch of fetched jobs.

    Returns the number of jobs successfully stored.
    """
    total_fetched = len(jobs)
    if total_fetched == 0:
        return 0

    try:
        t = time.perf_counter()
        load_learning_preferences(repository, profile)
        _stage_log("LEARNING_LOAD_TIME", t, "loaded user preferences")

        t = time.perf_counter()
        prepared_jobs = prepare_jobs_with_match_data(jobs, profile)
        _stage_log("MATCH_TIME", t, f"prepared={len(prepared_jobs)} jobs with match_data")

        t = time.perf_counter()
        filtered_jobs, filtered_count, avg_filter_score = filter_jobs(prepared_jobs, profile)
        _stage_log("FILTER_TIME", t, "finished filtering")
        logger.info(
            f"[FILTER] total={total_fetched} accepted={len(filtered_jobs)} "
            f"rejected={filtered_count} avg_score={avg_filter_score:.1f}"
        )

        t = time.perf_counter()
        unique_jobs, duplicate_count = deduplicate_jobs(filtered_jobs, repository)
        _stage_log("DEDUP_TIME", t, "finished dedup")
        logger.info(f"[DEDUP] unique={len(unique_jobs)} duplicates={duplicate_count}")

        t = time.perf_counter()
        enriched_jobs, avg_eval_score = enrich_jobs(unique_jobs, profile)
        _stage_log("SCORE_TIME", t, "finished scoring")
        logger.info(f"[SCORE] avg_score={avg_eval_score:.1f}")

        t = time.perf_counter()
        stored_jobs = store_jobs(enriched_jobs, repository)
        _stage_log("STORE_TIME", t, f"stored={len(stored_jobs)}")

        t = time.perf_counter()
        intelligence_count = generate_intelligence(stored_jobs, repository, profile)
        _stage_log("INTEL_TIME", t, f"intelligence={intelligence_count}")

        t = time.perf_counter()
        refresh_learning_preferences(repository, profile)
        _stage_log("LEARNING_UPDATE_TIME", t, "refreshed learning state")

        # Phase 5: generate embeddings when enabled (non-fatal)
        if settings.EMBEDDINGS_ENABLED and stored_jobs:
            try:
                from pipeline.embedder import get_embedding_provider
                t = time.perf_counter()
                provider = get_embedding_provider()
                newly_stored_ids = [j.job_id for j in stored_jobs]
                embed_jobs(newly_stored_ids, repository, provider)
                _stage_log("EMBED_TIME", t, f"embedded {len(newly_stored_ids)} new jobs")
            except Exception as e:
                logger.error(f"[EMBED_STAGE] Embedding stage failed (non-fatal): {e}")

        logger.info(
            f"[PIPELINE_SUMMARY] fetched={total_fetched} relevant={len(filtered_jobs)} "
            f"unique={len(unique_jobs)} stored={len(stored_jobs)} "
            f"intelligence={intelligence_count}"
        )
        return len(stored_jobs)

    except Exception as e:
        logger.exception(f"Fatal error in process_jobs: {e}")
        return 0


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_all_jobs() -> List[Job]:
    """Fetch jobs from all sources concurrently."""
    sources = [IndeedRSSFetcher(), AdzunaFetcher(), RemotiveFetcher()]
    all_jobs: List[Job] = []
    try:
        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            future_to_source = {
                executor.submit(source.fetch_and_normalize): (
                    source.__class__.__name__,
                    time.perf_counter(),
                )
                for source in sources
            }
            for future in as_completed(future_to_source):
                source_name, source_started = future_to_source[future]
                try:
                    jobs = future.result()
                    elapsed = time.perf_counter() - source_started
                    logger.info(
                        f"✅ {source_name}: {len(jobs)} jobs fetched in {elapsed:.2f}s"
                    )
                    all_jobs.extend(jobs)
                except Exception as e:
                    logger.exception(f"❌ {source_name} failed: {e}")
    except Exception as e:
        logger.exception(f"Threadpool error: {e}")
    logger.info(f"📥 Total fetched across all sources: {len(all_jobs)} jobs")
    if not all_jobs:
        logger.error("❌ CRITICAL: No jobs fetched from any source")
    return all_jobs
