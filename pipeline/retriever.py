"""
pipeline/retriever.py — Hybrid retrieval combining keyword score and vector similarity.

Two retrieval paths:
- retrieve_keyword_only(): keyword score only (always available, no embeddings needed)
- retrieve_hybrid(): fuses keyword_score + cosine_similarity(job_emb, profile_emb)

SQLite path: Python-side cosine computation on stored embedding_json blobs.
Postgres path: SQL with pgvector <=> cosine distance operator.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RetrievalResult:
    """Single retrieval result with component scores for explainability."""
    job_id: str
    keyword_score: float
    vector_score: float
    fused_score: float


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """
    Compute cosine similarity between two float vectors.
    Returns a float in [-1, 1]. Returns 0.0 if either vector is zero or empty.
    Both vectors must have the same length.
    """
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def fuse_scores(
    keyword_score: float,
    vector_score: float,
    kw_weight: float | None = None,
    vec_weight: float | None = None,
) -> float:
    """
    Combine keyword and vector scores into a single fused score in [0, 1].

    Args:
        keyword_score: Raw job score on 0-10 scale.
        vector_score:  Cosine similarity in [-1, 1]; negative values clamped to 0.
        kw_weight:     Keyword weight (default: settings.RETRIEVAL_KEYWORD_WEIGHT = 0.60).
        vec_weight:    Vector weight (default: settings.RETRIEVAL_VECTOR_WEIGHT = 0.40).

    Returns:
        Fused score in [0, 1].
    """
    from config import settings as _settings_module
    _s = _settings_module.settings
    kw_w = kw_weight if kw_weight is not None else _s.RETRIEVAL_KEYWORD_WEIGHT
    vec_w = vec_weight if vec_weight is not None else _s.RETRIEVAL_VECTOR_WEIGHT

    normalized_kw = max(0.0, min(1.0, keyword_score / 10.0))
    clamped_vec = max(0.0, min(1.0, float(vector_score)))
    return kw_w * normalized_kw + vec_w * clamped_vec


def retrieve_keyword_only(repository, limit: int = 30) -> list:
    """
    Return top jobs ranked by keyword score only.
    Falls back gracefully to an empty list if no jobs are stored.
    Always available — no embeddings required.
    """
    return repository.get_top_jobs(limit=limit)


def retrieve_hybrid(
    profile_embedding: List[float],
    repository,
    limit: int = 30,
    kw_weight: float | None = None,
    vec_weight: float | None = None,
) -> List[RetrievalResult]:
    """
    Retrieve and rank jobs using keyword score + vector similarity fusion.

    SQLite path: fetches jobs with embeddings, computes cosine similarity in Python.
    Postgres path: uses pgvector <=> operator in SQL.

    Falls back to keyword-only (with vector_score=0.0) if no profile embedding is provided.
    """
    if not profile_embedding:
        logger.warning("[RETRIEVER] No profile embedding — falling back to keyword-only")
        top_jobs = retrieve_keyword_only(repository, limit)
        return [
            RetrievalResult(
                job_id=j.job_id,
                keyword_score=j.score or 0.0,
                vector_score=0.0,
                fused_score=fuse_scores(j.score or 0.0, 0.0, kw_weight, vec_weight),
            )
            for j in top_jobs
        ]

    from config import settings as _settings_module
    is_postgres = _settings_module.settings.DB_BACKEND == "postgres"

    if is_postgres:
        return _retrieve_hybrid_postgres(profile_embedding, repository, limit, kw_weight, vec_weight)
    return _retrieve_hybrid_sqlite(profile_embedding, repository, limit, kw_weight, vec_weight)


def _retrieve_hybrid_sqlite(
    profile_embedding: List[float],
    repository,
    limit: int,
    kw_weight: float | None,
    vec_weight: float | None,
) -> List[RetrievalResult]:
    """SQLite path: Python-side cosine similarity on embedding_json blobs."""
    import json as _json
    from storage.db_manager import db_manager

    try:
        with db_manager.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, score, embedding_json
                FROM jobs
                WHERE embedding_json IS NOT NULL AND score IS NOT NULL
                ORDER BY score DESC
                LIMIT ?
                """,
                (limit * 10,),
            )
            rows = cursor.fetchall()
    except Exception as e:
        logger.error(f"[RETRIEVER] SQLite hybrid query failed: {e}")
        return []

    results: List[RetrievalResult] = []
    for row in rows:
        job_id, keyword_score, embedding_json = row
        if not embedding_json:
            continue
        try:
            job_embedding = _json.loads(embedding_json)
        except Exception:
            continue
        vec_score = cosine_similarity(profile_embedding, job_embedding)
        fused = fuse_scores(keyword_score or 0.0, vec_score, kw_weight, vec_weight)
        results.append(RetrievalResult(
            job_id=job_id,
            keyword_score=float(keyword_score or 0.0),
            vector_score=float(vec_score),
            fused_score=float(fused),
        ))

    results.sort(key=lambda r: r.fused_score, reverse=True)
    return results[:limit]


def _retrieve_hybrid_postgres(
    profile_embedding: List[float],
    repository,
    limit: int,
    kw_weight: float | None,
    vec_weight: float | None,
) -> List[RetrievalResult]:
    """Postgres path: SQL with pgvector cosine distance operator (<=>)."""
    from storage.db_manager import db_manager
    from config import settings as _settings_module
    _s = _settings_module.settings

    kw_w = kw_weight if kw_weight is not None else _s.RETRIEVAL_KEYWORD_WEIGHT
    vec_w = vec_weight if vec_weight is not None else _s.RETRIEVAL_VECTOR_WEIGHT
    vec_str = "[" + ",".join(f"{v:.8f}" for v in profile_embedding) + "]"

    query = """
        SELECT
            id,
            score,
            (1.0 - (embedding <=> ?::vector)) AS vector_score,
            (? * (score / 10.0) + ? * (1.0 - (embedding <=> ?::vector))) AS fused_score
        FROM jobs
        WHERE embedding IS NOT NULL AND score IS NOT NULL
        ORDER BY fused_score DESC
        LIMIT ?
    """
    try:
        with db_manager.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (vec_str, kw_w, vec_w, vec_str, limit))
            rows = cursor.fetchall()
    except Exception as e:
        logger.error(f"[RETRIEVER] Postgres hybrid query failed: {e}")
        return []

    return [
        RetrievalResult(
            job_id=row[0],
            keyword_score=float(row[1] or 0.0),
            vector_score=float(row[2] or 0.0),
            fused_score=float(row[3] or 0.0),
        )
        for row in rows
    ]
