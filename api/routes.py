"""
api/routes.py — FastAPI route for HVL Surface API.

POST /api/v1/process
────────────────────
Contract (ONE ZIP = ONE Order):
  1. Read ZIP → extract all supported files (pdf, eml, msg, txt)
  2. Run extraction on every file → merge into unified customer/order record
  3. Find or create Customer (never duplicate by name)
  4. Insert ONE Order  (NOrder: OFF-HVL-YYYY-NNNNNN)
  5. For EACH extracted item → upsert Item, insert OrderRow, write all
     OrderValues params 27–34 (default "No" when absent)
  6. Insert ProcessesList + PhasesList per OrderRow (best-effort)

Inputs (multipart/form-data):
  file          : ZIP file
  company_id    : int   — ID in Login.[dbo].Companies
  environment   : str   — prod | dev | uat | test
  modifier_user : str   — username stamped on all records (default: hvl_api)
  claude_key    : str   — optional Anthropic API key
  gemini_key    : str   — optional Google Gemini API key
  openai_key    : str   — optional OpenAI API key

Response JSON:
  {
    "NOrder"         : "OFF-HVL-2026-000025",
    "IdOrder"        : 25,
    "IdCustomer"     : 4,
    "company_db"     : "FlySolarTech_Etwin-dev",
    "environment"    : "dev",
    "files_processed": 4,
    "files_skipped"  : ["readme.txt"],
    "order_rows"     : [
      {
        "file"          : "3.037280.pdf",
        "IdItem"        : 3,
        "IdOrderRow"    : 29,
        "production_line": "L1",
        "surface_area_m2": "0.7222120",
        "IdProcessList" : 12,
        "IdPhaseList"   : [45, 46]
      },
      ...
    ],
    "errors": []
  }
"""
import io
import logging
import zipfile
from typing import List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from config import cfg
from db.connection import execute_query, get_connection
from db.operations import (
    find_or_create_customer,
    insert_order,
    insert_order_row,
    insert_order_values,
    insert_process_and_phases,
    upsert_item,
)
from extractor import extract_from_file

logger = logging.getLogger("hvl_api")

router = APIRouter(prefix="/api/v1")

SUPPORTED_EXTENSIONS = {"pdf", "eml", "msg", "txt"}
# Files whose names contain these strings are silently skipped
SKIP_NAME_FRAGMENTS  = {"readme", "__macosx", ".ds_store"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_identity_column(conn, table: str) -> str:
    """
    Detect the identity (auto-increment) or primary-key column of *table*.
    Tries INFORMATION_SCHEMA first, then primary-key constraint, then guesses.
    """
    for sql, params in [
        (
            """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
              AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME),COLUMN_NAME,'IsIdentity')=1
            """,
            (table,),
        ),
        (
            """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
              ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
             AND c.TABLE_NAME      = tc.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = ?
            """,
            (table,),
        ),
    ]:
        try:
            rows = execute_query(conn, sql, params)
            if rows:
                return rows[0]["COLUMN_NAME"]
        except Exception:
            pass

    for candidate in ("IdCompany", "CompanyId", "Id", "ID"):
        try:
            execute_query(conn, f"SELECT TOP 1 [{candidate}] FROM [dbo].[{table}]")
            return candidate
        except Exception:
            pass

    raise RuntimeError(f"Cannot determine PK column for [dbo].[{table}]")


def _resolve_company_db(login_conn, company_id: int, environment: str) -> str:
    """
    Look up DbName from Login.[dbo].Companies for *company_id*,
    then append the environment suffix if needed.
    """
    pk_col = _detect_identity_column(login_conn, "Companies")
    rows   = execute_query(
        login_conn,
        f"SELECT [DbName] FROM [dbo].[Companies] WHERE [{pk_col}] = ?",
        (company_id,),
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Company with ID {company_id} not found in Login DB",
        )

    db_name = rows[0]["DbName"]
    env     = environment.lower().strip()
    suffix  = f"-{env}"

    # Already has the correct suffix, or we're on prod (no suffix)
    if env == "prod" or db_name.lower().endswith(suffix):
        return db_name
    return f"{db_name}{suffix}"


def _list_supported_files(zf: zipfile.ZipFile) -> List[str]:
    """Return ZIP member names that are supported and not in the skip list."""
    return [
        name for name in zf.namelist()
        if not name.endswith("/")
        and name.rsplit(".", 1)[-1].lower() in SUPPORTED_EXTENSIONS
        and not any(skip in name.lower() for skip in SKIP_NAME_FRAGMENTS)
    ]


def _merge_extractions(extractions: List[dict]) -> dict:
    """
    Merge all per-file extractions into one unified dict.
    First non-empty value for each key wins; subsequent files only fill gaps.
    """
    merged: dict = {}
    for ext in extractions:
        for k, v in ext.items():
            if v is not None and str(v).strip() not in ("", "None", "null"):
                merged.setdefault(k, v)
    return merged


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@router.post("/process")
async def process_zip(
    file:          UploadFile      = File(..., description="ZIP containing PDFs and/or email files"),
    company_id:    int             = Form(...,  description="Company ID in Login.dbo.Companies"),
    environment:   str             = Form("dev", description="prod | dev | uat | test"),
    modifier_user: str             = Form("hvl_api", description="Username stamped on all records"),
    claude_key:    Optional[str]   = Form(None, description="Anthropic API key (optional)"),
    gemini_key:    Optional[str]   = Form(None, description="Google Gemini API key (optional)"),
    openai_key:    Optional[str]   = Form(None, description="OpenAI API key (optional)"),
):
    # ── Validate inputs ───────────────────────────────────────────────────────
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP files are accepted")

    env = environment.lower().strip()
    if env not in ("prod", "dev", "uat", "test"):
        raise HTTPException(
            status_code=400,
            detail="environment must be one of: prod, dev, uat, test",
        )

    # ── Read & validate ZIP ───────────────────────────────────────────────────
    raw_bytes = await file.read()
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw_bytes))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid ZIP archive")

    all_names  = [n for n in zf.namelist() if not n.endswith("/")]
    supported  = _list_supported_files(zf)
    skipped    = [n for n in all_names if n not in supported]

    logger.info(f"ZIP contains {len(all_names)} file(s), {len(supported)} supported")

    if not supported:
        raise HTTPException(
            status_code=400,
            detail="ZIP contains no supported files (pdf, eml, msg, txt)",
        )

    # ── Connect to Login DB → resolve company DB name ─────────────────────────
    login_db = cfg.login_db_for_env(env)
    try:
        login_conn = get_connection(
            cfg.db_server, cfg.db_port, login_db,
            cfg.db_user, cfg.db_password, cfg.connection_timeout,
        )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to login database ({login_db}): {e}",
        )

    try:
        company_db = _resolve_company_db(login_conn, company_id, env)
    finally:
        login_conn.close()

    # ── Connect to company DB ─────────────────────────────────────────────────
    try:
        conn = get_connection(
            cfg.db_server, cfg.db_port, company_db,
            cfg.db_user, cfg.db_password, cfg.connection_timeout,
        )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot connect to company database ({company_db}): {e}",
        )

    ai_keys = {
        "claude_key": claude_key or "",
        "gemini_key": gemini_key or "",
        "openai_key": openai_key or "",
    }

    # ── Extract from every supported file ─────────────────────────────────────
    extractions: List[dict] = []
    errors:      List[dict] = []

    for name in supported:
        logger.info(f"Extracting: {name}")
        file_bytes = zf.read(name)
        try:
            result = extract_from_file(
                filename=name,
                file_bytes=file_bytes,
                ai_keys=ai_keys,
            )
            extractions.append(result)
            logger.info(f"  ✓ Extracted: {name}")
        except Exception as e:
            logger.error(f"  ✗ Extraction failed for {name!r}: {e}", exc_info=True)
            errors.append({"file": name, "error": str(e)})

    if not extractions:
        conn.close()
        raise HTTPException(
            status_code=500,
            detail={"message": "All files failed extraction", "errors": errors},
        )

    # ── Merge extractions → unified customer / order metadata ─────────────────
    # All files in the ZIP belong to ONE offer.
    # Customer name, email, etc. come from whichever file has them.
    merged = _merge_extractions(extractions)
    logger.info(f"Merged data from {len(extractions)} file(s)")

    # ── DB pipeline ───────────────────────────────────────────────────────────
    try:
        # ONE customer per ZIP (find existing or create)
        id_customer = find_or_create_customer(conn, merged, modifier_user)

        # ONE order per ZIP
        id_order, norder = insert_order(conn, merged, modifier_user)
        logger.info(f"Created Order: {norder}")

        order_rows: List[dict] = []

        # One OrderRow + values + process per extracted item.
        # Skip files where no real ItemCode could be extracted (code derived from filename).
        for extraction in extractions:
            if extraction.get("_itemcode_derived"):
                logger.info(
                    f"  ↷ Skipping OrderRow for {extraction.get('source_file')!r} "
                    f"— ItemCode was derived from filename (no article code in document)"
                )
                continue
            id_item      = upsert_item(conn, extraction, modifier_user)
            id_order_row = insert_order_row(
                conn, id_order, id_item, id_customer, extraction, modifier_user
            )
            insert_order_values(conn, id_order_row, extraction, modifier_user)

            process_info = insert_process_and_phases(
                conn, id_order_row, id_item, extraction, modifier_user
            )

            logger.info(
                f"  ✓ OrderRow {id_order_row} for item {id_item} "
                f"| Line={extraction.get('production_line')} "
                f"| Surface={extraction.get('total_painting_surface')}"
            )

            order_rows.append({
                "file":            extraction.get("source_file", "unknown"),
                "IdItem":          id_item,
                "IdOrderRow":      id_order_row,
                "production_line": extraction.get("production_line", "No"),
                "surface_area_m2": extraction.get("total_painting_surface", "No"),
                "IdProcessList":   process_info.get("id_process_list"),
                "IdPhaseList":     process_info.get("id_phase_list", []),
            })

    except Exception as e:
        logger.error(f"DB pipeline failed: {e}", exc_info=True)
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    # ── Response ───────────────────────────────────────────────────────────────
    return JSONResponse(
        status_code=207 if errors else 200,
        content={
            "NOrder":          norder,
            "IdOrder":         id_order,
            "IdCustomer":      id_customer,
            "company_db":      company_db,
            "environment":     env,
            "files_processed": len(extractions),
            "files_skipped":   skipped,
            "order_rows":      order_rows,
            "errors":          errors,
        },
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@router.get("/health")
def health():
    return {"status": "ok", "service": "HVL Surface API", "version": "3.0.0"}
