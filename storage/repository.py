import sqlite3
from datetime import datetime
from typing import List, TYPE_CHECKING
from storage.db import get_connection
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


class JobRepository:
    """Repository for job data operations."""

    def _execute_with_retry(self, query, params=()):
        """Execute write queries with a simple retry mechanism."""
        conn = None
        for attempt in range(2):
            try:
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(query, params)
                conn.commit()
                return True
            except sqlite3.Error as e:
                logger.warning(f"DB retry {attempt + 1} failed: {e}")
            finally:
                if conn:
                    conn.close()
        return False

    def insert_job(self, job: 'Job') -> bool:
        """
        Insert a job into the database.

        Args:
            job: Job instance

        Returns:
            True if inserted successfully, False otherwise
        """
        query = """
            INSERT INTO jobs (
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
            job.posted_at.isoformat(),
            job.fetched_at.isoformat(),
            job.score,
            int(job.is_remote),
            int(job.is_startup)
        )

        try:
            return self._execute_with_retry(query, params)
        except sqlite3.IntegrityError:
            # Duplicate job ID
            return False
        except sqlite3.Error as e:
            logger.error(f"Failed to insert job {job.job_id}: {e}")
            return False

    def insert_hash(self, hash_value: str) -> bool:
        """
        Insert a job hash into the database.

        Args:
            hash_value: SHA-256 hash string

        Returns:
            True if inserted successfully, False otherwise
        """

        params = (hash_value, datetime.utcnow().isoformat())

        try:
            return self._execute_with_retry(query, params)
        except sqlite3.IntegrityError:
            # Hash already exists → treat as success
            return True
        except sqlite3.Error as e:
            logger.error(f"Failed to insert hash {hash_value[:8]}...: {e}")
            return False

    def hash_exists(self, hash_value: str) -> bool:
        """
        Check if a hash exists in the database.

        Args:
            hash_value: SHA-256 hash string

        Returns:
            True if exists, False otherwise
        """
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT 1 FROM job_hashes WHERE hash = ? LIMIT 1",
                (hash_value,)
            )
            result = cursor.fetchone()

            return result is not None

        except sqlite3.Error as e:
            logger.error(f"Failed to check hash {hash_value[:8]}...: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_top_jobs(self, limit: int = 30) -> List['Job']:
        """
        Get top jobs ordered by score.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of Job instances
        """
        from pipeline.models import Job  # avoid circular import

        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, title, company, location, description, url, source,
                       posted_at, fetched_at, score, is_remote, is_startup
                FROM jobs
                ORDER BY score DESC
                LIMIT ?
            """, (limit,))

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
                    posted_at=datetime.fromisoformat(row[7]),
                    fetched_at=datetime.fromisoformat(row[8])
                )
                job.score = row[9]
                job.is_remote = bool(row[10])
                job.is_startup = bool(row[11])
                jobs.append(job)

            return jobs

        except sqlite3.Error as e:
            logger.error(f"Failed to get top jobs: {e}")
            return []
        finally:
            if conn:
                conn.close()