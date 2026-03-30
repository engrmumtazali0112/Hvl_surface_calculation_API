"""
db/connection.py — SQL Server connection helpers
"""
import logging
import pyodbc
from typing import Optional

logger = logging.getLogger("db.connection")

# ---------------------------------------------------------------------------
# Driver resolution
# ---------------------------------------------------------------------------
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
    raise RuntimeError(f"No usable SQL Server ODBC driver found. Available: {available}")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def build_connection_string(
    server: str,
    port: int,
    database: str,
    user: str,
    password: str,
    timeout: int = 60,
) -> str:
    driver = _get_driver()
    # Use curly-brace quoting for server+port (handles special chars in hostnames)
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={{{server},{port}}};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Connection Timeout={timeout};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )


def get_connection(
    server: str,
    port: int,
    database: str,
    user: str,
    password: str,
    timeout: int = 60,
) -> pyodbc.Connection:
    cs = build_connection_string(server, port, database, user, password, timeout)
    logger.info(f"Connecting to SQL Server: {server},{port}/{database}")
    conn = pyodbc.connect(cs, timeout=timeout)
    conn.autocommit = False
    logger.info(f"✓ Connected to database: {database}")
    return conn


def execute_query(conn: pyodbc.Connection, sql: str, params: tuple = ()):
    """Execute a query and return all rows as a list of dicts."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        cols = [d[0] for d in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    except Exception as e:
        logger.error(f"Execute error: {e}")
        logger.error(f"SQL: {sql.strip()}")
        logger.error(f"Params: {params}")
        raise
    finally:
        cursor.close()


def execute_non_query(conn: pyodbc.Connection, sql: str, params: tuple = ()):
    """Execute INSERT/UPDATE/DELETE and return rowcount."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        rc = cursor.rowcount
        return rc
    except Exception as e:
        logger.error(f"Execute error: {e}")
        logger.error(f"SQL: {sql.strip()}")
        logger.error(f"Params: {params}")
        raise
    finally:
        cursor.close()