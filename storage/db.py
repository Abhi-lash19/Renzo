import sqlite3
from pathlib import Path

from utils.logger import get_logger

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "jobs.db"

def get_connection():
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    logger.info("Initializing database...")

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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS job_skills (
        job_id TEXT NOT NULL,
        skill TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (job_id, skill),
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS missing_skills (
        job_id TEXT NOT NULL,
        skill TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (job_id, skill),
        FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS job_hashes (
        hash TEXT PRIMARY KEY,
        created_at DATETIME
    )
    """)

    conn.commit()
    conn.close()

    logger.info("Database initialized successfully.")
