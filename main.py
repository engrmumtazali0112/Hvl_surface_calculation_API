"""
main.py — HVL Surface API entry point.

Start with:
  python -m uvicorn main:app --reload --port 8000
"""
import logging
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import cfg
from api.routes import router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S,%f"[:-3],
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="HVL Surface API",
    description=(
        "Processes ZIP files containing PDFs and email documents, "
        "extracts structured data, and persists it to the company database."
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


# ---------------------------------------------------------------------------
# Startup / shutdown events
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    cfg.print_config()
    logger.info("HVL Surface API starting up…")
    logger.info(f"Database engine: SQL Server ({cfg.db_server},{cfg.db_port})")

    # Test connection to the first Login DB
    try:
        from db.connection import get_connection, execute_query
        test_db = cfg.login_dbs[0]
        conn = get_connection(
            cfg.db_server, cfg.db_port, test_db,
            cfg.db_user, cfg.db_password, cfg.connection_timeout,
        )
        execute_query(conn, "SELECT DB_NAME()")
        rows = execute_query(conn, "SELECT COUNT(*) AS n FROM [dbo].[Companies]")
        company_count = rows[0]["n"] if rows else "?"
        conn.close()
        logger.info(f"✓ SQL Server connection successful — {company_count} companies found")
    except Exception as e:
        logger.error(f"✗ SQL Server connection failed: {e}")
        logger.warning("API will continue but DB operations may fail. Check connection settings.")


@app.on_event("shutdown")
async def shutdown():
    logger.info("HVL Surface API shutting down…")