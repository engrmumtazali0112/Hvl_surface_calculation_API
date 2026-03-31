"""
main.py — HVL Surface API entry point.

Start with:
    uvicorn main:app --reload --port 8000
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
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="HVL Surface API",
    description=(
        "Processes a ZIP file containing PDFs and email documents, "
        "extracts structured painting data, and persists it to the company database. "
        "ONE ZIP = ONE Order with all extracted items as OrderRows."
    ),
    version="3.0.0",
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
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    cfg.print_config()
    logger.info("HVL Surface API starting up…")
    logger.info(f"Database engine: SQL Server ({cfg.db_server},{cfg.db_port})")

    try:
        from db.connection import execute_query, get_connection
        test_db = cfg.login_dbs[0]           # e.g. "Login-dev"
        conn    = get_connection(
            cfg.db_server, cfg.db_port, test_db,
            cfg.db_user, cfg.db_password, cfg.connection_timeout,
        )
        rows    = execute_query(conn, "SELECT COUNT(*) AS n FROM [dbo].[Companies]")
        n       = rows[0]["n"] if rows else "?"
        conn.close()
        logger.info(f"✓ SQL Server OK — {n} companies found in {test_db}")
    except Exception as e:
        logger.error(f"✗ SQL Server connection failed: {e}")
        logger.warning("API will continue but DB operations may fail.")


@app.on_event("shutdown")
async def shutdown():
    logger.info("HVL Surface API shutting down…")
