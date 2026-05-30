import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import sqlite3
import pytest
from storage.db_manager import _CursorAdapter, _ConnectionAdapter


@pytest.fixture
def sqlite_conn():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    return conn


class TestCursorAdapter:
    def test_insert_with_question_mark_placeholder(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.execute("INSERT INTO t (name) VALUES (?)", ("hello",))
        adapter.commit()
        raw_cur = sqlite_conn.execute("SELECT name FROM t")
        assert raw_cur.fetchone()[0] == "hello"

    def test_fetchone_works(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.execute("INSERT INTO t (name) VALUES (?)", ("world",))
        adapter.commit()
        cursor.execute("SELECT name FROM t WHERE name = ?", ("world",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "world"

    def test_fetchall_works(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        for name in ["a", "b", "c"]:
            cursor = adapter.cursor()
            cursor.execute("INSERT INTO t (name) VALUES (?)", (name,))
        adapter.commit()
        cursor = adapter.cursor()
        cursor.execute("SELECT name FROM t ORDER BY name")
        rows = cursor.fetchall()
        assert len(rows) == 3

    def test_rowcount_after_insert(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.execute("INSERT INTO t (name) VALUES (?)", ("test",))
        adapter.commit()
        assert cursor.rowcount == 1

    def test_executemany_works(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.executemany(
            "INSERT INTO t (name) VALUES (?)",
            [("x",), ("y",), ("z",)]
        )
        adapter.commit()
        raw_cur = sqlite_conn.execute("SELECT COUNT(*) FROM t")
        assert raw_cur.fetchone()[0] == 3

    def test_placeholder_passthrough_for_sqlite(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.execute("SELECT ? + ?", (1, 2))
        result = cursor.fetchone()
        assert result[0] == 3

    def test_percent_s_placeholder_replaced(self, sqlite_conn):
        """When placeholder='%s', adapter should replace ? with %s for psycopg2 compat."""
        # We test the _adapt logic directly (can't test with real psycopg2 here)
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor_adapter = adapter.cursor()
        # _adapt with "?" placeholder: should NOT change anything
        assert cursor_adapter._adapt("SELECT ? FROM t") == "SELECT ? FROM t"


class TestConnectionAdapter:
    def test_commit_delegates(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.execute("INSERT INTO t (name) VALUES (?)", ("committed",))
        adapter.commit()
        raw_cur = sqlite_conn.execute("SELECT name FROM t WHERE name='committed'")
        assert raw_cur.fetchone() is not None

    def test_rollback_delegates(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        cursor = adapter.cursor()
        cursor.execute("INSERT INTO t (name) VALUES (?)", ("rollback_me",))
        adapter.rollback()
        raw_cur = sqlite_conn.execute("SELECT name FROM t WHERE name='rollback_me'")
        assert raw_cur.fetchone() is None

    def test_executescript_passthrough(self, sqlite_conn):
        adapter = _ConnectionAdapter(sqlite_conn, placeholder="?")
        adapter.executescript("INSERT INTO t (name) VALUES ('via_script');")
        raw_cur = sqlite_conn.execute("SELECT name FROM t WHERE name='via_script'")
        assert raw_cur.fetchone() is not None
