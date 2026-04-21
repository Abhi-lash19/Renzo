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
        query = """
            INSERT OR IGNORE INTO jobs (
                id, title, company, location, description, url, source,
                posted_at, fetched_at, score, is_remote, is_startup, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        except sqlite3.IntegrityError as e:
            logger.error(
                f"Database integrity error inserting job: {context} error={e}",
                extra={"component": "DB", "event": "insert_integrity_error",
                       "meta": {"job_id": job.job_id, "error": str(e)}}
            )
            return False
        except sqlite3.Error as e:
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
        except sqlite3.Error as e:
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
        query = "INSERT OR IGNORE INTO job_hashes (hash, created_at) VALUES (?, ?)"
        params = (hash_value, datetime.utcnow().isoformat())

        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                inserted = cursor.rowcount == 1
                if inserted:
                    return True
                return self.hash_exists(hash_value)
        except sqlite3.Error as e:
            logger.warning(
                f"Failed to insert hash {hash_value[:16]}...: {e}",
                extra={"component": "DB", "event": "hash_insert_error",
                       "meta": {"hash_prefix": hash_value[:16], "error": str(e)}}
            )
            return False

    def hash_exists(self, hash_value: str) -> bool:
        try:
            with db_manager.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM job_hashes WHERE hash = ? LIMIT 1",
                    (hash_value,),
                )
                return cursor.fetchone() is not None
        except sqlite3.Error as e:
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
        except sqlite3.Error as e:
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
        except sqlite3.Error as e:
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
        except sqlite3.Error as error:
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
        except sqlite3.Error as error:
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
        except sqlite3.Error as error:
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
                           posted_at, fetched_at, score, is_remote, is_startup
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
                job.skills = skill_map.get(job.job_id, [])
                job.missing_skills = missing_map.get(job.job_id, [])
                jobs.append(job)

            logger.info(
                f"[DB] Fetched top {len(jobs)} jobs",
                extra={"component": "DB", "event": "top_jobs_fetched",
                       "meta": {"count": len(jobs), "limit": limit}}
            )
            return jobs
        except sqlite3.Error as e:
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
        except sqlite3.Error as error:
            logger.error(
                f"Failed to fetch missing skill map: {error}",
                extra={"component": "DB", "event": "missing_skill_map_error",
                       "meta": {"error": str(error)}}
            )
            return {}
