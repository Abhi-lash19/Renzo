"""
GET /v1/jobs/top — Ranked job retrieval with hybrid or keyword scoring.

Uses hybrid retrieval when EMBEDDINGS_ENABLED=true and a profile embedding exists.
Falls back to keyword-only retrieval otherwise.
"""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, Query

from app.auth import get_current_user
from app.dependencies import get_repository
from app.schemas.job import JobTopResponse
from pipeline.retriever import RetrievalResult, fuse_scores, retrieve_hybrid, retrieve_keyword_only
from storage.repository import JobRepository
from utils.logger import get_logger

router = APIRouter(tags=["jobs"])
logger = get_logger(__name__)


def _keyword_jobs_to_responses(jobs: list, retrieval_mode: str = "keyword_only") -> List[JobTopResponse]:
    """Convert Job objects from keyword retrieval to JobTopResponse."""
    results = []
    for job in jobs:
        kw_score = float(getattr(job, "score", 0.0) or 0.0)
        fused = fuse_scores(kw_score, 0.0)
        results.append(JobTopResponse(
            job_id=job.job_id or "",
            title=job.title or "",
            company=job.company or "",
            location=job.location or "",
            url=job.url or "",
            score=kw_score,
            match_type=getattr(job, "match_type", "") or "",
            fused_score=fused,
            vector_score=0.0,
            retrieval_mode=retrieval_mode,
        ))
    return results


def _hybrid_results_to_responses(
    results: List[RetrievalResult],
    repository: JobRepository,
) -> List[JobTopResponse]:
    """Convert RetrievalResult objects to JobTopResponse, fetching job metadata from DB."""
    if not results:
        return []

    job_ids = [r.job_id for r in results]
    jobs_by_id: dict = {}
    try:
        from storage.db_manager import db_manager
        placeholders = ",".join("?" for _ in job_ids)
        with db_manager.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT id, title, company, location, url, match_type FROM jobs WHERE id IN ({placeholders})",
                tuple(job_ids),
            )
            for row in cursor.fetchall():
                jobs_by_id[row[0]] = {
                    "title": row[1] or "", "company": row[2] or "",
                    "location": row[3] or "", "url": row[4] or "",
                    "match_type": row[5] or "",
                }
    except Exception as e:
        logger.error(f"[JOBS_TOP] Failed to fetch job metadata: {e}")

    responses = []
    for result in results:
        meta = jobs_by_id.get(result.job_id, {})
        responses.append(JobTopResponse(
            job_id=result.job_id,
            title=meta.get("title", ""),
            company=meta.get("company", ""),
            location=meta.get("location", ""),
            url=meta.get("url", ""),
            score=result.keyword_score,
            match_type=meta.get("match_type", ""),
            fused_score=result.fused_score,
            vector_score=result.vector_score,
            retrieval_mode="hybrid",
        ))
    return responses


@router.get("/jobs/top", response_model=List[JobTopResponse])
def get_top_jobs(
    limit: int = Query(default=30, ge=1, le=100, description="Maximum jobs to return"),
    repository: JobRepository = Depends(get_repository),
    current_user: dict = Depends(get_current_user),
) -> List[JobTopResponse]:
    """
    Return top-ranked jobs.
    - EMBEDDINGS_ENABLED=false: keyword-score ranking only
    - EMBEDDINGS_ENABLED=true + profile embedding: hybrid keyword+vector ranking
    - EMBEDDINGS_ENABLED=true + no profile embedding: keyword-only with warning
    """
    from config import settings as _settings_module
    _s = _settings_module.settings

    if not _s.EMBEDDINGS_ENABLED:
        jobs = retrieve_keyword_only(repository, limit=limit)
        logger.debug(f"[JOBS_TOP] Keyword-only: {len(jobs)} jobs")
        return _keyword_jobs_to_responses(jobs, retrieval_mode="keyword_only")

    user_id = str(current_user.get("sub") or current_user.get("id", "anonymous"))
    profile_embedding = repository.get_profile_embedding(user_id)

    if not profile_embedding:
        logger.warning(f"[JOBS_TOP] No profile embedding for {user_id[:8]}... — using keyword-only")
        jobs = retrieve_keyword_only(repository, limit=limit)
        return _keyword_jobs_to_responses(jobs, retrieval_mode="keyword_only")

    results = retrieve_hybrid(profile_embedding, repository, limit=limit)
    logger.info(f"[JOBS_TOP] Hybrid retrieval: {len(results)} results")

    if not results:
        jobs = retrieve_keyword_only(repository, limit=limit)
        return _keyword_jobs_to_responses(jobs, retrieval_mode="keyword_only")

    return _hybrid_results_to_responses(results, repository)
