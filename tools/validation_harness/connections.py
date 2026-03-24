"""
Connection factory for validation-harness.

Supports:
  - SQL Server  (mssql+pyodbc://)
  - SQLite      (sqlite://)       — used for unit tests / local dev
  - Databricks  (databricks://)   — requires databricks-sql-connector
  - Athena      (awsathena+rest://) — requires PyAthena
  - Generic SQLAlchemy DSN

The `Connection` abstraction exposes only:
  .execute(sql)  → returns an object with .scalar() and .fetchall()
  .close()

This thin wrapper lets tests inject a MockConnection without touching
the runner or rule logic.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class Connection:
    """Minimal DB connection interface used by recon / rule evaluators."""

    def execute(self, sql: str, params: Optional[dict] = None) -> "Result":
        raise NotImplementedError

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Result:
    """Thin wrapper around a DB cursor result."""

    def scalar(self) -> Any:
        raise NotImplementedError

    def fetchall(self) -> List[Sequence]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# SQLAlchemy implementation
# ---------------------------------------------------------------------------

class SQLAlchemyConnection(Connection):
    """Production connection backed by SQLAlchemy."""

    def __init__(self, dsn: str):
        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            raise RuntimeError(
                "sqlalchemy is required for real DB connections. "
                "Install it with: pip install sqlalchemy"
            )
        self._engine = create_engine(dsn)
        self._conn = self._engine.connect()
        self._text = text
        log.debug("Connected to %s", dsn.split("@")[-1])   # host only, no creds

    def execute(self, sql: str, params: Optional[dict] = None) -> "SQLAlchemyResult":
        stmt = self._text(sql)
        result = self._conn.execute(stmt, params or {})
        return SQLAlchemyResult(result)

    def close(self) -> None:
        self._conn.close()


class SQLAlchemyResult(Result):
    def __init__(self, result):
        self._result = result

    def scalar(self) -> Any:
        row = self._result.fetchone()
        return row[0] if row else None

    def fetchall(self) -> List[Sequence]:
        return self._result.fetchall()


# ---------------------------------------------------------------------------
# Mock implementation (for tests and dry-run mode)
# ---------------------------------------------------------------------------

class MockResult(Result):
    def __init__(self, rows: List[Sequence]):
        self._rows = rows

    def scalar(self) -> Any:
        return self._rows[0][0] if self._rows else None

    def fetchall(self) -> List[Sequence]:
        return self._rows


class MockConnection(Connection):
    """
    In-memory connection for unit tests.

    Usage:
        conn = MockConnection()
        conn.set_result("SELECT COUNT(*) FROM orders", [(1000,)])
        conn.set_result("SELECT COUNT(*) FROM orders WHERE id IS NULL", [(0,)])
    """

    def __init__(self):
        self._results: dict = {}
        self._default_rows: List[Sequence] = [(0,)]
        self.executed: List[str] = []    # audit trail of SQL executed

    def set_result(self, sql_fragment: str, rows: List[Sequence]) -> None:
        """Register a result for any SQL containing sql_fragment."""
        self._results[sql_fragment.strip().lower()] = rows

    def set_default(self, rows: List[Sequence]) -> None:
        """Default result for any SQL not explicitly registered."""
        self._default_rows = rows

    def execute(self, sql: str, params: Optional[dict] = None) -> MockResult:
        self.executed.append(sql)
        sql_lower = sql.strip().lower()
        for key, rows in self._results.items():
            if key in sql_lower:
                return MockResult(rows)
        return MockResult(self._default_rows)

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_connection(dsn: str, mock: bool = False) -> Connection:
    """
    Create a Connection from a DSN string.

    Args:
        dsn:  SQLAlchemy-style DSN or "mock://" for a MockConnection.
        mock: If True, always return a MockConnection regardless of DSN.
    """
    if mock or dsn.startswith("mock://"):
        return MockConnection()
    return SQLAlchemyConnection(dsn)
