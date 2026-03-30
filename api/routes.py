"""
api/routes.py — FastAPI routes for HVL Surface API.

POST /api/v1/process
  - Accepts a ZIP file containing PDFs and/or .eml/.msg files
  - ONE ZIP = ONE Order + ONE OrderRow
  - ALL items detected (from all files) are linked to that single order
  - Customer is matched by name first; only inserted if not found
  - OrderValues params 27–33 are always written (0 if missing)

  company_id   : ID in Login.[dbo].Companies → resolves DB name
  environment  : prod | dev | uat | test
  modifier_user: username to stamp on all records
  (optional) claude_key, gemini_key, openai_key for AI extraction
"""
import io
import logging
import zipfile
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from config import cfg
from db.connection import get_connection, execute_query
from db.operations import (
    upsert_item,
    find_or_create_customer,
    insert_order,
    insert_order_row,
    insert_order_values,
)
from extractor import extract_from_file

logger = logging.getLogger("hvl_api")

router = APIRouter(prefix="/api/v1")


# ---------------------------------------------------------------------------
# Helper: resolve company DB name
# ---------------------------------------------------------------------------

def _detect_pk_column(conn, table_name: str) -> str:
    try:
        rows = execute_query(
            conn,
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
              AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME),
                                 COLUMN_NAME, 'IsIdentity') = 1
            """,
            (table_name,),
        )
        if rows:
            return rows[0]["COLUMN_NAME"]
    except Exception as e:
        logger.warning(f"Identity column detection failed: {e}")

    try:
        rows = execute_query(
            conn,
            """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
              ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
             AND c.TABLE_NAME      = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND tc.TABLE_NAME      = ?
            """,
            (table_name,),
        )
        if rows:
            return rows[0]["COLUMN_NAME"]
    except Exception as e:
        logger.warning(f"PK detection failed: {e}")

    for candidate in ("IdCompany", "CompanyId", "Id", "ID", "id"):
        try:
            execute_query(conn, f"SELECT TOP 1 [{candidate}] FROM [dbo].[{table_name}]")
            return candidate
        except Exception:
            continue

    raise RuntimeError(f"Cannot determine PK column for [dbo].[{table_name}].")


def _resolve_company(login_conn, company_id: int, environment: str) -> str:
    pk_col = _detect_pk_column(login_conn, "Companies")
    rows = execute_query(
        login_conn,
        f"SELECT [DbName] FROM [dbo].[Companies] WHERE [{pk_col}] = ?",
        (company_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Company with ID {company_id} not found")

    db_name_raw = rows[0]["DbName"]
    env = environment.lower().strip()
    env_suffix = f"-{env}"

    if db_name_raw.lower().endswith(env_suffix) or env == "prod":
        return db_name_raw
    return f"{db_name_raw}{env_suffix}"


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@router.post("/process")
async def process_zip(
    file: UploadFile = File(..., description="ZIP containing PDFs and/or .eml/.msg files"),
    company_id:    int  = Form(..., description="Company ID in Login.dbo.Companies"),
    environment:   str  = Form("dev", description="prod | dev | uat | test"),
    modifier_user: str  = Form("hvl_api", description="Username stamped on all records"),
    claude_key:    Optional[str] = Form(None),
    gemini_key:    Optional[str] = Form(None),
    openai_key:    Optional[str] = Form(None),
):
    """
    Process a ZIP file:
      - Extract data from ALL PDFs/emails inside
      - Create ONE Order for the entire ZIP
      - Find or create the Customer (never duplicate by name)
      - Create ONE OrderRow per extracted item
      - Write ALL OrderValues params 27–33 (0 if value is missing)
    """
    # ── 1. Read ZIP ──────────────────────────────────────────────────────────
    raw = await file.read()
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid ZIP")

    names = [n for n in zf.namelist() if not n.endswith("/")]
    supported = [n for n in names if n.rsplit(".", 1)[-1].lower() in ("pdf", "eml", "msg", "txt")]
    logger.info(f"ZIP contains {len(names)} file(s), {len(supported)} supported")

    if not supported:
        raise HTTPException(status_code=400, detail="ZIP contains no supported files (pdf, eml, msg, txt)")

    # ── 2. Connect to Login DB ────────────────────────────────────────────────
    login_db = cfg.login_db_for_env(environment)
    try:
        login_conn = get_connection(cfg.db_server, cfg.db_port, login_db,
                                    cfg.db_user, cfg.db_password, cfg.connection_timeout)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Cannot connect to login database: {e}")

    try:
        company_db = _resolve_company(login_conn, company_id, environment)
    finally:
        login_conn.close()

    # ── 3. Connect to company DB ──────────────────────────────────────────────
    try:
        company_conn = get_connection(cfg.db_server, cfg.db_port, company_db,
                                      cfg.db_user, cfg.db_password, cfg.connection_timeout)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Cannot connect to company database: {e}")

    ai_keys = {
        "claude_key": claude_key,
        "gemini_key": gemini_key,
        "openai_key": openai_key,
    }

    # ── 4. Extract data from ALL files ────────────────────────────────────────
    extractions = []
    errors = []

    for name in supported:
        logger.info(f"Extracting: {name}")
        file_bytes = zf.read(name)
        try:
            extraction = extract_from_file(
                filename=name,
                file_bytes=file_bytes,
                ai_keys=ai_keys,
            )
            extractions.append(extraction)
            logger.info(f"  ✓ Extracted: {name}")
        except Exception as e:
            logger.error(f"  ✗ Extraction failed for {name!r}: {e}", exc_info=True)
            errors.append({"file": name, "error": str(e)})

    if not extractions:
        company_conn.close()
        raise HTTPException(status_code=500, detail={"message": "All files failed to extract", "errors": errors})

    # ── 5. Merge all extractions into one unified record ──────────────────────
    # First extraction is the base; subsequent ones fill in any missing fields
    merged = {}
    for ext in extractions:
        for k, v in ext.items():
            if v is not None and v != "" and k not in merged:
                merged[k] = v

    logger.info(f"Merged data from {len(extractions)} file(s)")

    # ── 6. ONE Order for the entire ZIP ──────────────────────────────────────
    try:
        # Find or create customer (smart match by name — never blindly insert)
        id_customer = find_or_create_customer(company_conn, merged, modifier_user)

        # Create ONE order
        id_order, norder = insert_order(company_conn, merged, modifier_user)
        logger.info(f"Created Order: {norder}")

        # Create ONE OrderRow per extracted item (all under the same Order)
        order_rows = []
        for extraction in extractions:
            id_item = upsert_item(company_conn, extraction, modifier_user)
            id_order_row = insert_order_row(
                company_conn, id_order, id_item, id_customer, extraction, modifier_user
            )
            # Write ALL 7 OrderValues (params 27–33), default to "0" if missing
            insert_order_values(company_conn, id_order_row, extraction, modifier_user)
            order_rows.append({
                "file":        extraction.get("source_file", "unknown"),
                "IdItem":      id_item,
                "IdOrderRow":  id_order_row,
            })
            logger.info(f"  ✓ OrderRow {id_order_row} for item {id_item}")

    except Exception as e:
        logger.error(f"DB pipeline failed: {e}", exc_info=True)
        company_conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    company_conn.close()

    # ── 7. Response ───────────────────────────────────────────────────────────
    return JSONResponse(
        status_code=207 if errors else 200,
        content={
            "company_db":   company_db,
            "environment":  environment,
            "NOrder":       norder,
            "IdOrder":      id_order,
            "IdCustomer":   id_customer,
            "files_processed": len(extractions),
            "order_rows":   order_rows,
            "errors":       errors,
        },
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@router.get("/health")
def health():
    return {"status": "ok", "service": "HVL Surface API"}