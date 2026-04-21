"""
Central Database Manager — single source of truth for all DB access.

Responsibilities:
  - Lazy, thread-safe connection management (SQLite shared connection)
  - Postgres-ready design (pool acquire/release)
  - Centralized execute/executemany with commit control
  - Graceful shutdown

Usage:
  from storage.db_manager import db_manager
  db_manager.execute("INSERT INTO ...", (param1, param2))
  rows = db_manager.execute("SELECT ...", fetch=True)
"""

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_DB_PATH = DATA_DIR / "jobs.db"

DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()          # "sqlite" | "postgres"
DB_PATH = os.getenv("DB_PATH", str(DEFAULT_DB_PATH))      # SQLite file path
PG_DSN = os.getenv("PG_DSN", "")                          # e.g. "host=localhost dbname=renzo user=..."
PG_POOL_MIN = int(os.getenv("PG_POOL_MIN", "2"))
PG_POOL_MAX = int(os.getenv("PG_POOL_MAX", "10"))


class DatabaseManager:
    """
    Thread-safe database manager.

    SQLite mode  : single shared connection protected by a lock.
    Postgres mode: connection pool (acquire/release per operation).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sqlite_conn: Optional[sqlite3.Connection] = None
        self._pg_pool: Any = None  # psycopg2.pool.SimpleConnectionPool
        self._initialized = False
        self._db_type = DB_TYPE

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_init(self) -> None:
        """Lazy initialization — called on first access."""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            if self._db_type == "postgres":
                self._init_postgres()
            else:
                self._init_sqlite()
            self._initialized = True

    def _init_sqlite(self) -> None:
        db_path = Path(DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        abs_path = db_path.resolve()

        self._sqlite_conn = sqlite3.connect(str(abs_path), check_same_thread=False)
        self._sqlite_conn.execute("PRAGMA journal_mode=WAL")
        self._sqlite_conn.execute("PRAGMA foreign_keys = ON")
        self._sqlite_conn.execute("PRAGMA busy_timeout = 5000")

        logger.info(
            f"[DB] SQLite connection initialized",
            extra={"component": "DB", "event": "init_sqlite", "meta": {"path": str(abs_path)}}
        )

    def _init_postgres(self) -> None:
        if not PG_DSN:
            logger.warning(
                "[DB] PG_DSN not set — falling back to SQLite",
                extra={"component": "DB", "event": "pg_fallback"}
            )
            self._db_type = "sqlite"
            self._init_sqlite()
            return

        try:
            import psycopg2.pool  # type: ignore
            self._pg_pool = psycopg2.pool.SimpleConnectionPool(
                PG_POOL_MIN, PG_POOL_MAX, PG_DSN
            )
            logger.info(
                f"[DB] Postgres pool initialized",
                extra={"component": "DB", "event": "init_postgres",
                       "meta": {"min": PG_POOL_MIN, "max": PG_POOL_MAX}}
            )
        except ImportError:
            logger.warning(
                "[DB] psycopg2 not installed — falling back to SQLite",
                extra={"component": "DB", "event": "pg_import_fail"}
            )
            self._db_type = "sqlite"
            self._init_sqlite()
        except Exception as e:
            logger.error(
                f"[DB] Postgres pool creation failed — falling back to SQLite: {e}",
                extra={"component": "DB", "event": "pg_pool_fail", "meta": {"error": str(e)}}
            )
            self._db_type = "sqlite"
            self._init_sqlite()

    # ------------------------------------------------------------------
    # Connection access
    # ------------------------------------------------------------------

    @contextmanager
    def connection(self):
        """
        Context manager that yields a DB connection.

        SQLite  : returns shared connection (caller must NOT close it).
        Postgres: acquires from pool, returns to pool on exit.
        """
        self._ensure_init()

        if self._db_type == "postgres" and self._pg_pool:
            conn = self._pg_pool.getconn()
            try:
                yield conn
            finally:
                self._pg_pool.putconn(conn)
        else:
            with self._lock:
                yield self._sqlite_conn

    def get_connection(self):
        """
        Backward-compatible: returns raw connection.
        Callers should NOT close the connection — it's managed centrally.
        """
        self._ensure_init()
        if self._db_type == "postgres" and self._pg_pool:
            return self._pg_pool.getconn()
        return self._sqlite_conn

    def return_connection(self, conn) -> None:
        """Return a Postgres connection to the pool. No-op for SQLite."""
        if self._db_type == "postgres" and self._pg_pool and conn:
            try:
                self._pg_pool.putconn(conn)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(
        self,
        query: str,
        params: Union[Tuple, None] = None,
        fetch: bool = False,
        commit: bool = True,
    ) -> Union[List[Tuple], int]:
        """
        Execute a single query.

        Args:
            query:  SQL string
            params: bind parameters
            fetch:  if True, return fetchall() result
            commit: if True, commit after write operations

        Returns:
            List of rows if fetch=True, else rowcount.
        """
        self._ensure_init()

        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                if fetch:
                    return cursor.fetchall()
                if commit:
                    conn.commit()
                return cursor.rowcount
            except Exception:
                if commit:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise

    def executemany(
        self,
        query: str,
        params_list: List[Tuple],
        commit: bool = True,
    ) -> int:
        """
        Execute a batch query.

        Returns:
            Total rowcount.
        """
        self._ensure_init()

        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(query, params_list)
                if commit:
                    conn.commit()
                return cursor.rowcount
            except Exception:
                if commit:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise

    def execute_script(self, script: str) -> None:
        """Execute a raw SQL script (for schema creation / migrations)."""
        self._ensure_init()
        with self.connection() as conn:
            conn.executescript(script) if self._db_type == "sqlite" else conn.cursor().execute(script)
            conn.commit()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        """Gracefully close all connections."""
        with self._lock:
            if self._sqlite_conn:
                try:
                    self._sqlite_conn.close()
                    logger.info(
                        "[DB] SQLite connection closed",
                        extra={"component": "DB", "event": "shutdown_sqlite"}
                    )
                except Exception as e:
                    logger.warning(f"[DB] Error closing SQLite connection: {e}")
                self._sqlite_conn = None

            if self._pg_pool:
                try:
                    self._pg_pool.closeall()
                    logger.info(
                        "[DB] Postgres pool closed",
                        extra={"component": "DB", "event": "shutdown_postgres"}
                    )
                except Exception as e:
                    logger.warning(f"[DB] Error closing Postgres pool: {e}")
                self._pg_pool = None

            self._initialized = False

    def __del__(self) -> None:
        try:
            self.shutdown()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
db_manager = DatabaseManager()
