"""
Job repository — all DB operations for job data.

All access goes through DatabaseManager. No direct sqlite3.connect() calls.
No manual conn.close() — connection lifecycle is centrally managed.
"""

import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, TYPE_CHECKING

from storage.db_manager import db_manager
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)
VALID_INTERACTIONS = {"viewed", "applied", "ignored"}


class JobRepository:
    """Repository for job data operations."""

    def _job_context(self, job: "Job") -> str:
        return (
            f"job_id={job.job_id or 'unknown'} "
            f"title={job.title or 'Unknown'} "
            f"source={job.source or 'Unknown'}"
        )

    def _serialize_datetime(self, value):
        if value is None:
            return None
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    def insert_job(self, job: "Job") -> bool:
        if not job.job_id or not job.title or not job.company or not job.url:
            logger.warning(
                f"Invalid job data, cannot store: job_id={job.job_id or 'missing'} "
                f"title={job.title or 'missing'} company={job.company or 'missing'} url={job.url or 'missing'}",
                extra={"component": "DB", "event": "insert_skip_invalid",
                       "meta": {"job_id": job.job_id or "missing"}}
            )
            return False

        context = self._job_context(job)
        from config.settings import settings as _settings
        is_postgres = _settings.DB_BACKEND == "postgres"
        if is_postgres:
            query = """
                INSERT INTO jobs (
                    id, title, company, location, description, url, source,
                    posted_at, fetched_at, score, is_remote, is_startup, updated_at, match_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO NOTHING
            """
        else:
            query = """
                INSERT OR IGNORE INTO jobs (
                    id, title, company, location, description, url, source,
                    posted_at, fetched_at, score, is_remote, is_startup, updated_at, match_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        now_str = datetime.utcnow().isoformat()
        params = (
            job.job_id,
            job.title,
            job.company,
            job.location,
            job.description,
            job.url,
            job.source,
            self._serialize_datetime(job.posted_at),
            self._serialize_datetime(job.fetched_at),
            job.score,
            int(job.is_remote),
            int(job.is_startup),
            now_str,
            getattr(job, "match_type", ""),
        )

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                if cursor.rowcount == 0:
                    logger.debug(
                        f"DB insert skipped duplicate: {context}",
                        extra={"component": "DB", "event": "insert_duplicate",
                               "meta": {"job_id": job.job_id}}
                    )
                    return False
                logger.debug(
                    f"DB insert succeeded: {context}",
                    extra={"component": "DB", "event": "insert_success",
                           "meta": {"job_id": job.job_id}}
                )
                return True
        except Exception as e:
            logger.error(
                f"Database integrity error inserting job: {context} error={e}",
                extra={"component": "DB", "event": "insert_integrity_error",
                       "meta": {"job_id": job.job_id, "error": str(e)}}
            )
            return False
        except Exception as e:
            logger.error(
                f"Failed to insert job: {context} error={e}",
                extra={"component": "DB", "event": "insert_error",
                       "meta": {"job_id": job.job_id, "error": str(e)}}
            )
            return False

    def _replace_job_items(self, table: str, job_id: str, values: Iterable[str]) -> bool:
        clean_values = list(dict.fromkeys(value for value in values if value))
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DELETE FROM {table} WHERE job_id = ?", (job_id,))
                if clean_values:
                    cursor.executemany(
                        f"INSERT INTO {table} (job_id, skill) VALUES (?, ?)",
                        [(job_id, value) for value in clean_values],
                    )
                conn.commit()
                return True
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(
                f"Failed to update {table} for job {job_id}: {e}",
                extra={"component": "DB", "event": "replace_items_error",
                       "meta": {"table": table, "job_id": job_id, "error": str(e)}}
            )
            return False

    def insert_skills(self, job_id: str, skills: Iterable[str]) -> bool:
        return self._replace_job_items("job_skills", job_id, skills)

    def insert_missing_skills(self, job_id: str, skills: Iterable[str]) -> bool:
        return self._replace_job_items("missing_skills", job_id, skills)

    def insert_hash(self, hash_value: str) -> bool:
        """Return True if newly inserted; False if already existed; True on error (conservative)."""
        from config.settings import settings as _settings
        is_postgres = _settings.DB_BACKEND == "postgres"
        if is_postgres:
            query = """
                INSERT INTO job_hashes (hash, created_at)
                VALUES (?, ?)
                ON CONFLICT (hash) DO NOTHING
            """
        else:
            query = "INSERT OR IGNORE INTO job_hashes (hash, created_at) VALUES (?, ?)"
        params = (hash_value, datetime.utcnow().isoformat())

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                # rowcount==1: newly inserted (new hash). rowcount==0: already existed (duplicate).
                return cursor.rowcount == 1
        except Exception as e:
            logger.warning(
                f"Failed to insert hash {hash_value[:16]}...: {e}",
                extra={"component": "DB", "event": "hash_insert_error",
                       "meta": {"hash_prefix": hash_value[:16], "error": str(e)}}
            )
            # Return True (conservative): treat as if newly inserted to avoid unbounded re-processing
            # of a job whose hash was never persisted.
            return True

    def hash_exists(self, hash_value: str) -> bool:
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM job_hashes WHERE hash = ? LIMIT 1",
                    (hash_value,),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(
                f"Failed to check hash {hash_value[:16]}...: {e}",
                extra={"component": "DB", "event": "hash_check_error",
                       "meta": {"hash_prefix": hash_value[:16], "error": str(e)}}
            )
            return False

    def update_job_score(self, job_id: str, score: float) -> bool:
        now_str = datetime.utcnow().isoformat()
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE jobs SET score = ?, updated_at = ? WHERE id = ?",
                    (score, now_str, job_id),
                )
                conn.commit()
                if cursor.rowcount == 0:
                    logger.error(
                        f"Score update affected no rows for job {job_id}",
                        extra={"component": "DB", "event": "score_update_miss",
                               "meta": {"job_id": job_id}}
                    )
                    return False
                return True
        except Exception as e:
            logger.error(
                f"Failed to update score for job {job_id}: {e}",
                extra={"component": "DB", "event": "score_update_error",
                       "meta": {"job_id": job_id, "error": str(e)}}
            )
            return False

    def _get_job_items(self, table: str, job_id: str) -> List[str]:
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT skill FROM {table} WHERE job_id = ? ORDER BY skill ASC", (job_id,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(
                f"Failed to fetch {table} for job {job_id}: {e}",
                extra={"component": "DB", "event": "fetch_items_error",
                       "meta": {"table": table, "job_id": job_id, "error": str(e)}}
            )
            return []

    def get_job_skills(self, job_id: str) -> List[str]:
        return self._get_job_items("job_skills", job_id)

    def get_missing_skills(self, job_id: str) -> List[str]:
        return self._get_job_items("missing_skills", job_id)

    def record_interaction(self, job_id: str, action: str) -> bool:
        normalized_action = (action or "").strip().lower()
        if not job_id or normalized_action not in VALID_INTERACTIONS:
            logger.warning(
                f"[INTERACTION_RECORD] invalid interaction job_id={job_id or 'missing'} action={action}",
                extra={"component": "DB", "event": "interaction_invalid",
                       "meta": {"job_id": job_id or "missing", "action": action}}
            )
            return False

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO user_interactions (job_id, action, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (job_id, normalized_action, datetime.utcnow().isoformat()),
                )
                conn.commit()
                logger.info(
                    f"[INTERACTION_RECORD] job_id={job_id} action={normalized_action} recorded=True",
                    extra={"component": "DB", "event": "interaction_recorded",
                           "meta": {"job_id": job_id, "action": normalized_action}}
                )
                return True
        except Exception as error:
            logger.error(
                f"[INTERACTION_RECORD] job_id={job_id} action={normalized_action} error={error}",
                extra={"component": "DB", "event": "interaction_error",
                       "meta": {"job_id": job_id, "action": normalized_action, "error": str(error)}}
            )
            return False

    def _get_job_skill_map(self, job_ids: List[str]) -> Dict[str, List[str]]:
        if not job_ids:
            return {}

        placeholders = ",".join("?" for _ in job_ids)
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                    SELECT job_id, skill
                    FROM job_skills
                    WHERE job_id IN ({placeholders})
                    ORDER BY job_id ASC, skill ASC
                    """,
                    tuple(job_ids),
                )
                skill_map: Dict[str, List[str]] = defaultdict(list)
                for job_id, skill in cursor.fetchall():
                    if skill:
                        skill_map[job_id].append(skill)
                return {job_id: list(dict.fromkeys(skills)) for job_id, skills in skill_map.items()}
        except Exception as error:
            logger.error(
                f"Failed to fetch skill map for interactions: {error}",
                extra={"component": "DB", "event": "skill_map_error",
                       "meta": {"error": str(error)}}
            )
            return {}

    def get_interaction_jobs(self, actions: Iterable[str] | None = None, limit: int = 200) -> List[Dict[str, object]]:
        normalized_actions = [
            action.strip().lower()
            for action in (actions or VALID_INTERACTIONS)
            if action and action.strip().lower() in VALID_INTERACTIONS
        ]
        if not normalized_actions:
            return []

        placeholders = ",".join("?" for _ in normalized_actions)
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                    SELECT ui.job_id, ui.action, ui.created_at, j.title, j.company, j.location, j.description
                    FROM user_interactions ui
                    JOIN jobs j ON j.id = ui.job_id
                    WHERE ui.action IN ({placeholders})
                    ORDER BY ui.created_at DESC
                    LIMIT ?
                    """,
                    (*normalized_actions, limit),
                )
                rows = cursor.fetchall()
                job_ids = list(dict.fromkeys(row[0] for row in rows if row and row[0]))
                skill_map = self._get_job_skill_map(job_ids)

                snapshots: List[Dict[str, object]] = []
                for row in rows:
                    job_id, action, created_at, title, company, location, description = row
                    snapshots.append({
                        "job_id": job_id,
                        "action": action,
                        "created_at": created_at,
                        "title": title or "",
                        "company": company or "",
                        "location": location or "",
                        "description": description or "",
                        "skills": list(skill_map.get(job_id, [])),
                    })
                return snapshots
        except Exception as error:
            logger.error(
                f"Failed to fetch user interaction jobs: {error}",
                extra={"component": "DB", "event": "interaction_jobs_error",
                       "meta": {"error": str(error)}}
            )
            return []

    def get_top_jobs(self, limit: int = 30) -> List["Job"]:
        from pipeline.models import Job

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT id, title, company, location, description, url, source,
                           posted_at, fetched_at, score, is_remote, is_startup, match_type
                    FROM jobs
                    ORDER BY score DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()

            # Batch-fetch skills for all jobs instead of N+1 queries
            job_ids = [row[0] for row in rows if row[0]]
            skill_map = self._get_job_skill_map(job_ids) if job_ids else {}
            missing_map = self._get_missing_skill_map(job_ids) if job_ids else {}

            jobs = []
            for row in rows:
                job = Job(
                    job_id=row[0],
                    title=row[1],
                    company=row[2],
                    location=row[3],
                    description=row[4],
                    url=row[5],
                    source=row[6],
                    posted_at=datetime.fromisoformat(row[7]) if row[7] else None,
                    fetched_at=datetime.fromisoformat(row[8]) if row[8] else None,
                )
                job.score = row[9]
                job.is_remote = bool(row[10])
                job.is_startup = bool(row[11])
                job.match_type = row[12] or ""
                job.skills = skill_map.get(job.job_id, [])
                job.missing_skills = missing_map.get(job.job_id, [])
                jobs.append(job)

            logger.info(
                f"[DB] Fetched top {len(jobs)} jobs",
                extra={"component": "DB", "event": "top_jobs_fetched",
                       "meta": {"count": len(jobs), "limit": limit}}
            )
            return jobs
        except Exception as e:
            logger.error(
                f"Failed to get top jobs: {e}",
                extra={"component": "DB", "event": "top_jobs_error",
                       "meta": {"error": str(e)}}
            )
            return []

    def _get_missing_skill_map(self, job_ids: List[str]) -> Dict[str, List[str]]:
        """Batch-fetch missing skills for multiple jobs (avoids N+1)."""
        if not job_ids:
            return {}

        placeholders = ",".join("?" for _ in job_ids)
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                    SELECT job_id, skill
                    FROM missing_skills
                    WHERE job_id IN ({placeholders})
                    ORDER BY job_id ASC, skill ASC
                    """,
                    tuple(job_ids),
                )
                skill_map: Dict[str, List[str]] = defaultdict(list)
                for job_id, skill in cursor.fetchall():
                    if skill:
                        skill_map[job_id].append(skill)
                return {job_id: list(dict.fromkeys(skills)) for job_id, skills in skill_map.items()}
        except Exception as error:
            logger.error(
                f"Failed to fetch missing skill map: {error}",
                extra={"component": "DB", "event": "missing_skill_map_error",
                       "meta": {"error": str(error)}}
            )
            return {}

    # -----------------------------------------------------------------------
    # Job run queue
    # -----------------------------------------------------------------------

    _VALID_RUN_STATUSES = {"queued", "running", "complete", "failed"}

    def create_job_run(self, run_id: str, user_id: str | None = None) -> bool:
        """Insert a new job run with status='queued'. Returns False if run_id already exists.
        user_id is stored when DB_BACKEND=postgres."""
        from config.settings import settings
        is_postgres = settings.DB_BACKEND == "postgres"

        if is_postgres and user_id:
            query = """
                INSERT INTO job_runs (run_id, status, created_at, user_id)
                VALUES (?, 'queued', ?, ?)
                ON CONFLICT (run_id) DO NOTHING
            """
            params = (run_id, datetime.utcnow().isoformat(), user_id)
        elif is_postgres:
            query = """
                INSERT INTO job_runs (run_id, status, created_at)
                VALUES (?, 'queued', ?)
                ON CONFLICT (run_id) DO NOTHING
            """
            params = (run_id, datetime.utcnow().isoformat())
        else:
            query = """
                INSERT OR IGNORE INTO job_runs (run_id, status, created_at)
                VALUES (?, 'queued', ?)
            """
            params = (run_id, datetime.utcnow().isoformat())

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                inserted = cursor.rowcount == 1
                logger.debug(
                    f"[JOB_RUN] create run_id={run_id} inserted={inserted}",
                    extra={"component": "DB", "event": "job_run_create",
                           "meta": {"run_id": run_id, "inserted": inserted}}
                )
                return inserted
        except Exception as e:
            logger.error(
                f"[JOB_RUN] Failed to create run run_id={run_id}: {e}",
                extra={"component": "DB", "event": "job_run_create_error",
                       "meta": {"run_id": run_id, "error": str(e)}}
            )
            return False

    def update_run_status(
        self,
        run_id: str,
        status: str,
        started_at: str | None = None,
        completed_at: str | None = None,
        result_json: str | None = None,
    ) -> bool:
        """Update status and optional timestamps/result. Returns False if not found or invalid status."""
        if status not in self._VALID_RUN_STATUSES:
            logger.warning(
                f"[JOB_RUN] Invalid status={status} for run_id={run_id}",
                extra={"component": "DB", "event": "job_run_invalid_status",
                       "meta": {"run_id": run_id, "status": status}}
            )
            return False

        query = """
            UPDATE job_runs
            SET status = ?,
                started_at = COALESCE(?, started_at),
                completed_at = COALESCE(?, completed_at),
                result_json = COALESCE(?, result_json)
            WHERE run_id = ?
        """
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (status, started_at, completed_at, result_json, run_id))
                conn.commit()
                if cursor.rowcount == 0:
                    logger.warning(
                        f"[JOB_RUN] No row updated for run_id={run_id}",
                        extra={"component": "DB", "event": "job_run_update_miss",
                               "meta": {"run_id": run_id}}
                    )
                    return False
                logger.debug(
                    f"[JOB_RUN] updated run_id={run_id} status={status}",
                    extra={"component": "DB", "event": "job_run_updated",
                           "meta": {"run_id": run_id, "status": status}}
                )
                return True
        except Exception as e:
            logger.error(
                f"[JOB_RUN] Failed to update run_id={run_id}: {e}",
                extra={"component": "DB", "event": "job_run_update_error",
                       "meta": {"run_id": run_id, "error": str(e)}}
            )
            return False

    def get_job_run(self, run_id: str) -> dict | None:
        """Fetch a job run row by run_id. Returns None if not found."""
        query = """
            SELECT run_id, status, created_at, started_at, completed_at, result_json
            FROM job_runs
            WHERE run_id = ?
        """
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (run_id,))
                row = cursor.fetchone()
                if row is None:
                    return None
                return {
                    "run_id": row[0],
                    "status": row[1],
                    "created_at": row[2],
                    "started_at": row[3],
                    "completed_at": row[4],
                    "result_json": row[5],
                }
        except Exception as e:
            logger.error(
                f"[JOB_RUN] Failed to fetch run_id={run_id}: {e}",
                extra={"component": "DB", "event": "job_run_fetch_error",
                       "meta": {"run_id": run_id, "error": str(e)}}
            )
            return None

    # -----------------------------------------------------------------------
    # User profile storage
    # -----------------------------------------------------------------------

    def upsert_profile(
        self,
        user_id: str,
        profile_json: str,
        raw_text: str = "",
        source: str = "upload",
        name: str = "",
        role: str = "",
        experience_level: str = "",
    ) -> bool:
        """
        Insert or replace the user's profile (keyed by user_id).
        SQLite: INSERT OR REPLACE. Postgres: INSERT ... ON CONFLICT (user_id) DO UPDATE.
        Returns True on success, False on failure.
        """
        import uuid
        from config.settings import settings as _settings

        profile_id = str(uuid.uuid4())
        now_str = datetime.utcnow().isoformat()
        is_postgres = _settings.DB_BACKEND == "postgres"

        params = (
            profile_id, user_id, name, role, experience_level,
            raw_text, profile_json, source, now_str, now_str,
        )

        if is_postgres:
            query = """
                INSERT INTO user_profiles
                    (id, user_id, name, role, experience_level,
                     raw_text, profile_json, source, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    role = EXCLUDED.role,
                    experience_level = EXCLUDED.experience_level,
                    raw_text = EXCLUDED.raw_text,
                    profile_json = EXCLUDED.profile_json,
                    source = EXCLUDED.source,
                    updated_at = EXCLUDED.updated_at
            """
            try:
                with db_manager.connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    conn.commit()
                    logger.info(
                        f"[PROFILE] Upserted profile for user_id={user_id[:8]}...",
                        extra={"component": "DB", "event": "profile_upsert",
                               "meta": {"user_id": user_id[:8], "source": source}}
                    )
                    return True
            except Exception as e:
                logger.error(
                    f"[PROFILE] Failed to upsert for user_id={user_id[:8]}...: {e}",
                    extra={"component": "DB", "event": "profile_upsert_error",
                           "meta": {"user_id": user_id[:8], "error": str(e)}}
                )
                return False
        else:
            # SQLite: INSERT OR IGNORE preserves created_at on existing rows;
            # the subsequent UPDATE refreshes all mutable fields.
            insert_query = """
                INSERT OR IGNORE INTO user_profiles
                    (id, user_id, name, role, experience_level,
                     raw_text, profile_json, source, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            update_query = """
                UPDATE user_profiles
                SET name = ?, role = ?, experience_level = ?,
                    raw_text = ?, profile_json = ?, source = ?, updated_at = ?
                WHERE user_id = ?
            """
            try:
                with db_manager.connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(insert_query, params)
                    cursor.execute(update_query, (
                        name, role, experience_level,
                        raw_text, profile_json, source, now_str,
                        user_id,
                    ))
                    conn.commit()
                    logger.info(
                        f"[PROFILE] Upserted profile for user_id={user_id[:8]}...",
                        extra={"component": "DB", "event": "profile_upsert",
                               "meta": {"user_id": user_id[:8], "source": source}}
                    )
                    return True
            except Exception as e:
                logger.error(
                    f"[PROFILE] Failed to upsert for user_id={user_id[:8]}...: {e}",
                    extra={"component": "DB", "event": "profile_upsert_error",
                           "meta": {"user_id": user_id[:8], "error": str(e)}}
                )
                return False

    def get_profile_by_user(self, user_id: str) -> dict | None:
        """Fetch stored profile for a user. Returns None if not found."""
        query = """
            SELECT id, user_id, name, role, experience_level,
                   raw_text, profile_json, source, created_at, updated_at
            FROM user_profiles
            WHERE user_id = ?
            LIMIT 1
        """
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (user_id,))
                row = cursor.fetchone()
                if row is None:
                    return None
                return {
                    "id": row[0], "user_id": row[1], "name": row[2],
                    "role": row[3], "experience_level": row[4],
                    "raw_text": row[5], "profile_json": row[6],
                    "source": row[7], "created_at": row[8], "updated_at": row[9],
                }
        except Exception as e:
            logger.error(
                f"[PROFILE] Failed to fetch for user_id={user_id[:8]}...: {e}",
                extra={"component": "DB", "event": "profile_fetch_error",
                       "meta": {"user_id": user_id[:8], "error": str(e)}}
            )
            return None

    # -----------------------------------------------------------------------
    # Job embedding storage (Phase 5)
    # -----------------------------------------------------------------------

    def store_job_embedding(self, job_id: str, embedding: list) -> bool:
        """Store a job embedding. SQLite: JSON blob. Postgres: VECTOR type."""
        import json as _json
        from config.settings import settings as _settings

        is_postgres = _settings.DB_BACKEND == "postgres"
        if is_postgres:
            vec_str = "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"
            query = "UPDATE jobs SET embedding = ?::vector WHERE id = ?"
            params = (vec_str, job_id)
        else:
            query = "UPDATE jobs SET embedding_json = ? WHERE id = ?"
            params = (_json.dumps(embedding), job_id)

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                logger.debug(
                    f"[EMBEDDER] Stored embedding for job_id={job_id}",
                    extra={"component": "EMBEDDER", "event": "job_embedding_stored",
                           "meta": {"job_id": job_id}}
                )
                return True
        except Exception as e:
            logger.error(
                f"[EMBEDDER] Failed to store embedding for job_id={job_id}: {e}",
                extra={"component": "EMBEDDER", "event": "job_embedding_error",
                       "meta": {"job_id": job_id, "error": str(e)}}
            )
            return False

    def get_job_embedding(self, job_id: str) -> list | None:
        """Fetch stored embedding for a job. Returns None if not found."""
        import json as _json
        from config.settings import settings as _settings

        is_postgres = _settings.DB_BACKEND == "postgres"
        col = "embedding" if is_postgres else "embedding_json"
        query = f"SELECT {col} FROM jobs WHERE id = ? LIMIT 1"

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (job_id,))
                row = cursor.fetchone()
                if not row or row[0] is None:
                    return None
                raw = row[0]
                if is_postgres:
                    return list(raw) if not isinstance(raw, list) else raw
                return _json.loads(raw)
        except Exception as e:
            logger.error(
                f"[EMBEDDER] Failed to get embedding for job_id={job_id}: {e}",
                extra={"component": "EMBEDDER", "event": "job_embedding_fetch_error",
                       "meta": {"job_id": job_id, "error": str(e)}}
            )
            return None

    def get_jobs_without_embeddings(self, limit: int = 100) -> list:
        """Return list of job_ids that have no stored embedding."""
        from config.settings import settings as _settings

        is_postgres = _settings.DB_BACKEND == "postgres"
        if is_postgres:
            query = "SELECT id FROM jobs WHERE embedding IS NULL LIMIT ?"
        else:
            query = "SELECT id FROM jobs WHERE embedding_json IS NULL LIMIT ?"

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (limit,))
                return [row[0] for row in cursor.fetchall() if row[0]]
        except Exception as e:
            logger.error(
                f"[EMBEDDER] Failed to get un-embedded jobs: {e}",
                extra={"component": "EMBEDDER", "event": "jobs_without_embeddings_error",
                       "meta": {"error": str(e)}}
            )
            return []

    # -----------------------------------------------------------------------
    # Profile embedding storage (Phase 5)
    # -----------------------------------------------------------------------

    def store_profile_embedding(self, user_id: str, embedding: list) -> bool:
        """Store the embedding for a user's profile."""
        import json as _json
        from config.settings import settings as _settings

        is_postgres = _settings.DB_BACKEND == "postgres"
        if is_postgres:
            vec_str = "[" + ",".join(f"{v:.8f}" for v in embedding) + "]"
            query = "UPDATE user_profiles SET profile_embedding = ?::vector WHERE user_id = ?"
            params = (vec_str, user_id)
        else:
            query = "UPDATE user_profiles SET profile_embedding_json = ? WHERE user_id = ?"
            params = (_json.dumps(embedding), user_id)

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                logger.debug(
                    f"[EMBEDDER] Stored profile embedding for user_id={user_id[:8]}...",
                    extra={"component": "EMBEDDER", "event": "profile_embedding_stored",
                           "meta": {"user_id": user_id[:8]}}
                )
                return True
        except Exception as e:
            logger.error(
                f"[EMBEDDER] Failed to store profile embedding: {e}",
                extra={"component": "EMBEDDER", "event": "profile_embedding_error",
                       "meta": {"user_id": user_id[:8] if user_id else "?", "error": str(e)}}
            )
            return False

    def get_profile_embedding(self, user_id: str) -> list | None:
        """Fetch stored profile embedding. Returns None if not found."""
        import json as _json
        from config.settings import settings as _settings

        is_postgres = _settings.DB_BACKEND == "postgres"
        col = "profile_embedding" if is_postgres else "profile_embedding_json"
        query = f"SELECT {col} FROM user_profiles WHERE user_id = ? LIMIT 1"

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (user_id,))
                row = cursor.fetchone()
                if not row or row[0] is None:
                    return None
                raw = row[0]
                if is_postgres:
                    return list(raw) if not isinstance(raw, list) else raw
                return _json.loads(raw)
        except Exception as e:
            logger.error(
                f"[EMBEDDER] Failed to get profile embedding: {e}",
                extra={"component": "EMBEDDER", "event": "profile_embedding_fetch_error",
                       "meta": {"user_id": user_id[:8] if user_id else "?", "error": str(e)}}
            )
            return None
