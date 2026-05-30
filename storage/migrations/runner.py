"""
MigrationRunner — applies numbered SQL migration files to a Postgres connection.

Usage (with a live psycopg2 connection):
    from storage.migrations.runner import MigrationRunner
    from pathlib import Path
    runner = MigrationRunner(Path("storage/migrations"))
    runner.apply_to_connection(psycopg2_conn)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)


class MigrationRunner:
    """Discovers and applies numbered SQL migration files in lexicographic order."""

    def __init__(self, migrations_dir: Path) -> None:
        self.migrations_dir = Path(migrations_dir)

    def load_migrations(self) -> List[Tuple[str, str]]:
        """
        Return list of (filename, sql_content) tuples sorted by filename.
        Only files matching 0*.sql are included.
        """
        files = sorted(self.migrations_dir.glob("0*.sql"))
        migrations = []
        for file in files:
            sql = file.read_text(encoding="utf-8").strip()
            if sql:
                migrations.append((file.name, sql))
        return migrations

    def apply_to_connection(self, conn) -> None:
        """
        Apply all migrations in order to the given psycopg2 connection.
        Each migration is applied in its own transaction. Already-applied migrations
        are tracked in the schema_migrations table (idempotent re-runs are safe).

        Args:
            conn: An open psycopg2 connection.
        """
        migrations = self.load_migrations()
        if not migrations:
            logger.warning("[MIGRATION] No migration files found in %s", self.migrations_dir)
            return

        with conn.cursor() as cursor:
            # Ensure migration tracking table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.commit()

            for name, sql in migrations:
                version = name.replace(".sql", "")
                cursor.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = %s",
                    (version,)
                )
                if cursor.fetchone():
                    logger.debug("[MIGRATION] Skipping already-applied: %s", name)
                    continue

                logger.info("[MIGRATION] Applying: %s", name)
                try:
                    cursor.execute(sql)
                    cursor.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)",
                        (version,)
                    )
                    conn.commit()
                    logger.info("[MIGRATION] Applied: %s", name)
                except Exception as e:
                    conn.rollback()
                    logger.error("[MIGRATION] Failed on %s: %s", name, e)
                    raise RuntimeError(f"Migration {name} failed: {e}") from e
