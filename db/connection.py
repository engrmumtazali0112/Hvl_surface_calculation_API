"""
db/connection.py — SQL Server connection helpers.
"""
import logging
import pyodbc
from typing import List, Dict, Any

logger = logging.getLogger("db.connection")

_PREFERRED_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server",
]


def _get_driver() -> str:
    available = pyodbc.drivers()
    for d in _PREFERRED_DRIVERS:
        if d in available:
            return d
    raise RuntimeError(f"No SQL Server ODBC driver found. Available: {available}")


def build_connection_string(
    server: str, port: int, database: str,
    user: str, password: str, timeout: int = 60,
) -> str:
    driver = _get_driver()
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={{{server},{port}}};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        f"Connection Timeout={timeout};"
        f"TrustServerCertificate=yes;Encrypt=yes;"
    )


def get_connection(
    server: str, port: int, database: str,
    user: str, password: str, timeout: int = 60,
) -> pyodbc.Connection:
    cs   = build_connection_string(server, port, database, user, password, timeout)
    logger.info(f"Connecting to SQL Server: {server},{port}/{database}")
    conn = pyodbc.connect(cs, timeout=timeout)
    conn.autocommit = False
    logger.info(f"✓ Connected to database: {database}")
    return conn


def execute_query(
    conn: pyodbc.Connection, sql: str, params: tuple = ()
) -> List[Dict[str, Any]]:
    """Execute a SELECT and return rows as list-of-dicts."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        cols = [d[0] for d in cursor.description] if cursor.description else []
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Query error: {e}\nSQL: {sql.strip()}\nParams: {params}")
        raise
    finally:
        cursor.close()


def execute_non_query(
    conn: pyodbc.Connection, sql: str, params: tuple = ()
) -> int:
    """Execute INSERT/UPDATE/DELETE and return rowcount."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        return cursor.rowcount
    except Exception as e:
        logger.error(f"Non-query error: {e}\nSQL: {sql.strip()}\nParams: {params}")
        raise
    finally:
        cursor.close()
