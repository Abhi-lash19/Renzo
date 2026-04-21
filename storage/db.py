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


def check_column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    return column_name in columns


def apply_migrations(conn):
    cursor = conn.cursor()
    logger.info("[DB MIGRATION] Starting database migrations...")
    
    # 1. Add Columns to jobs safely
    columns_to_add = [
        ("jobs", "updated_at", "DATETIME"),
        ("jobs", "raw_json", "TEXT")
    ]
    
    for table, col_name, col_type in columns_to_add:
        if not check_column_exists(cursor, table, col_name):
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                logger.info(f"[DB MIGRATION] Adding column: {col_name}")
            except Exception as e:
                logger.error(f"[DB MIGRATION] Failed to add column {col_name}: {e}")
                
    # 2. Add Indexes
    indexes = [
        ("idx_jobs_source", "jobs(source)"),
        ("idx_jobs_posted_at", "jobs(posted_at)"),
        ("idx_jobs_score", "jobs(score)"),
        ("idx_jobs_status", "jobs(status)"),
        ("idx_job_skills_job_id", "job_skills(job_id)"),
        ("idx_missing_skills_job_id", "missing_skills(job_id)"),
        ("idx_user_interactions_job_id", "user_interactions(job_id)"),
        ("idx_user_interactions_action", "user_interactions(action)"),
        ("idx_user_interactions_created_at", "user_interactions(created_at)")
    ]
    
    for idx_name, idx_def in indexes:
        try:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")
            logger.info(f"[DB MIGRATION] Index created: {idx_name}")
        except Exception as e:
            logger.error(f"[DB MIGRATION] Failed to create index {idx_name}: {e}")
            
    # 3. Create UNIQUE Index for URL
    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_url_unique ON jobs(url)")
        logger.info("[DB MIGRATION] Index created: idx_jobs_url_unique")
    except Exception as e:
        logger.error(f"[DB MIGRATION] Failed to create unique index idx_jobs_url_unique: {e}")
        
    logger.info("[DB MIGRATION] Migrations completed successfully.")


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

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_interactions (
            job_id TEXT NOT NULL,
            action TEXT NOT NULL CHECK(action IN ('viewed', 'applied', 'ignored')),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )
        """)
        logger.debug("[DB] Table 'user_interactions' ensured")
        
        apply_migrations(conn)

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
