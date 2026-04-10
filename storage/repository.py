import sqlite3
from datetime import datetime
from typing import Iterable, List, TYPE_CHECKING

from storage.db import get_connection
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


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
                f"title={job.title or 'missing'} company={job.company or 'missing'} url={job.url or 'missing'}"
            )
            return False

        context = self._job_context(job)
        query = """
            INSERT OR IGNORE INTO jobs (
                id, title, company, location, description, url, source,
                posted_at, fetched_at, score, is_remote, is_startup
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
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
        )

        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            if cursor.rowcount == 0:
                logger.debug(f"DB insert skipped duplicate: {context}")
                return False
            logger.debug(f"DB insert succeeded: {context}")
            return True
        except sqlite3.IntegrityError as e:
            logger.error(f"Database integrity error inserting job: {context} error={e}")
            if conn:
                conn.rollback()
            return False
        except sqlite3.Error as e:
            logger.error(f"Failed to insert job: {context} error={e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def _replace_job_items(self, table: str, job_id: str, values: Iterable[str]) -> bool:
        clean_values = list(dict.fromkeys(value for value in values if value))
        conn = None
        try:
            conn = get_connection()
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
            logger.error(f"Failed to update {table} for job {job_id}: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def insert_skills(self, job_id: str, skills: Iterable[str]) -> bool:
        return self._replace_job_items("job_skills", job_id, skills)

    def insert_missing_skills(self, job_id: str, skills: Iterable[str]) -> bool:
        return self._replace_job_items("missing_skills", job_id, skills)

    def insert_hash(self, hash_value: str) -> bool:
        query = "INSERT OR IGNORE INTO job_hashes (hash, created_at) VALUES (?, ?)"
        params = (hash_value, datetime.utcnow().isoformat())

        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            inserted = cursor.rowcount == 1
            if inserted:
                return True
            return self.hash_exists(hash_value)
        except sqlite3.Error as e:
            logger.warning(f"Failed to insert hash {hash_value[:16]}...: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def hash_exists(self, hash_value: str) -> bool:
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM job_hashes WHERE hash = ? LIMIT 1",
                (hash_value,),
            )
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Failed to check hash {hash_value[:16]}...: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def update_job_score(self, job_id: str, score: float) -> bool:
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE jobs SET score = ? WHERE id = ?", (score, job_id))
            conn.commit()
            if cursor.rowcount == 0:
                logger.error(f"Score update affected no rows for job {job_id}")
                return False
            return True
        except sqlite3.Error as e:
            logger.error(f"Failed to update score for job {job_id}: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def _get_job_items(self, table: str, job_id: str) -> List[str]:
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT skill FROM {table} WHERE job_id = ? ORDER BY skill ASC", (job_id,))
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Failed to fetch {table} for job {job_id}: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_job_skills(self, job_id: str) -> List[str]:
        return self._get_job_items("job_skills", job_id)

    def get_missing_skills(self, job_id: str) -> List[str]:
        return self._get_job_items("missing_skills", job_id)

    def get_top_jobs(self, limit: int = 30) -> List["Job"]:
        from pipeline.models import Job

        conn = None
        try:
            conn = get_connection()
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
                job.skills = self.get_job_skills(job.job_id)
                job.missing_skills = self.get_missing_skills(job.job_id)
                jobs.append(job)

            return jobs
        except sqlite3.Error as e:
            logger.error(f"Failed to get top jobs: {e}")
            return []
        finally:
            if conn:
                conn.close()
