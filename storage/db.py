import sqlite3
import os
from utils.logger import get_logger

logger = get_logger(__name__)

DB_PATH = "data/jobs.db"

def get_connection():
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect(DB_PATH)


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
    CREATE TABLE IF NOT EXISTS job_hashes (
        hash TEXT PRIMARY KEY,
        created_at DATETIME
    )
    """)

    conn.commit()
    conn.close()

    logger.info("Database initialized successfully.")