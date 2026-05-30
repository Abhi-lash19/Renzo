import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from pathlib import Path as P


MIGRATIONS_DIR = P(__file__).resolve().parent.parent / "storage" / "migrations"


class TestMigrationRunner:
    def test_migrations_dir_exists(self):
        assert MIGRATIONS_DIR.exists()

    def test_four_sql_files_exist(self):
        sql_files = sorted(MIGRATIONS_DIR.glob("0*.sql"))
        assert len(sql_files) == 4

    def test_migration_files_in_order(self):
        sql_files = sorted(MIGRATIONS_DIR.glob("0*.sql"))
        names = [f.name for f in sql_files]
        assert names[0].startswith("001_")
        assert names[1].startswith("002_")
        assert names[2].startswith("003_")
        assert names[3].startswith("004_")

    def test_runner_loads_migration_files(self):
        from storage.migrations.runner import MigrationRunner
        runner = MigrationRunner(MIGRATIONS_DIR)
        migrations = runner.load_migrations()
        assert len(migrations) == 4
        for name, sql in migrations:
            assert name.endswith(".sql")
            assert len(sql.strip()) > 0

    def test_migration_001_has_jobs_table(self):
        sql_file = MIGRATIONS_DIR / "001_initial_schema.sql"
        sql = sql_file.read_text()
        assert "CREATE TABLE" in sql.upper()
        assert "jobs" in sql.lower()

    def test_migration_002_has_user_id_column(self):
        sql_file = MIGRATIONS_DIR / "002_add_users.sql"
        sql = sql_file.read_text()
        assert "user_id" in sql.lower()

    def test_migration_003_has_vector(self):
        sql_file = MIGRATIONS_DIR / "003_pgvector.sql"
        sql = sql_file.read_text()
        assert "vector" in sql.lower()

    def test_migration_003_has_1024_dims(self):
        sql_file = MIGRATIONS_DIR / "003_pgvector.sql"
        sql = sql_file.read_text()
        assert "1024" in sql

    def test_migration_004_has_policy(self):
        sql_file = MIGRATIONS_DIR / "004_rls_policies.sql"
        sql = sql_file.read_text()
        assert "policy" in sql.lower()

    def test_runner_has_apply_to_connection_method(self):
        from storage.migrations.runner import MigrationRunner
        runner = MigrationRunner(MIGRATIONS_DIR)
        assert hasattr(runner, "apply_to_connection")

    def test_runner_has_load_migrations_method(self):
        from storage.migrations.runner import MigrationRunner
        runner = MigrationRunner(MIGRATIONS_DIR)
        assert hasattr(runner, "load_migrations")

    def test_runner_migration_names_are_sorted(self):
        from storage.migrations.runner import MigrationRunner
        runner = MigrationRunner(MIGRATIONS_DIR)
        migrations = runner.load_migrations()
        names = [m[0] for m in migrations]
        assert names == sorted(names)
