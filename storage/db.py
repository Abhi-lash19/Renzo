import sqlite3
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "jobs.db"

def get_connection():
    try:
        DATA_DIR.mkdir(exist_ok=True)

        abs_path = DB_PATH.resolve()
        logger.info(f"[DB] Connecting to database at: {abs_path}")

        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("PRAGMA foreign_keys = ON")

        logger.debug("[DB] Connection established successfully")

        return conn

    except Exception as e:
        logger.exception(f"[DB] Failed to create connection: {e}")
        raise


def init_db():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        logger.info("[DB] Initializing database...")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            title TEXT,
            company TEXT,
            location TEXT,
            description TEXT,
            url TEXT,
            source TEXT,
            posted_at DATETIME,
            fetched_at DATETIME,
            score REAL,
            is_remote INTEGER,
            is_startup INTEGER,
            status TEXT DEFAULT 'not_applied'
        )
        """)
        logger.debug("[DB] Table 'jobs' ensured")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_skills (
            job_id TEXT NOT NULL,
            skill TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (job_id, skill),
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
        """)
        logger.debug("[DB] Table 'job_skills' ensured")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS missing_skills (
            job_id TEXT NOT NULL,
            skill TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (job_id, skill),
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
        """)
        logger.debug("[DB] Table 'missing_skills' ensured")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_hashes (
            hash TEXT PRIMARY KEY,
            created_at DATETIME
        )
        """)
        logger.debug("[DB] Table 'job_hashes' ensured")

        conn.commit()
        logger.info("[DB] Database initialized and committed successfully")

    except Exception as e:
        logger.exception(f"[DB] Error initializing database: {e}")
        raise

    finally:
        if conn:
            try:
                conn.close()
                logger.debug("[DB] Connection closed after init")
            except Exception:
                logger.warning("[DB] Failed to close connection cleanly")