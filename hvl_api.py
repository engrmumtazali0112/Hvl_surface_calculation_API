"""
HVL Surface API — Task 2047
============================
POST /api/v1/process
Accepts a ZIP file + company context, extracts HVL data, and inserts into SQLite.
"""

# ── Standard library ─────────────────────────────────────────────────────────
import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import List, Dict, Any

# ── Third-party ──────────────────────────────────────────────────────────────
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── Local ─────────────────────────────────────────────────────────────────────
from db import Database
from extractor import FileExtractor

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("hvl_api")

# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="HVL Surface API",
    description="Extract HVL data from ZIP and insert into database",
    version="3.0.0",
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
SUPPORTED_EXT = {".pdf", ".eml", ".txt"}
SKIP_NAMES = {"readme", "__macosx", ".ds_store"}

ENV_LOGIN_DB = {
    "prod": "Login",
    "dev": "Login-dev",
    "uat": "Login-uat",
    "test": "Login-test",
}

ORDER_PARAMS = {
    27: "ral_color",
    28: "finishing_type",
    29: "batch_size",
    30: "pitch_mm",
    31: "protections_present",
    32: "protection_type",
    33: "total_surface_area_m2",
}

# ─────────────────────────────────────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────────────────────────────────────
class OrderResponse(BaseModel):
    source_file: str
    NOrder: str
    IdOrder: int
    IdOrderRow: int
    IdItem: int
    IdCustomer: int


class ProcessResponse(BaseModel):
    status: str
    company_id: int
    company_db: str
    environment: str
    files_processed: int
    files_skipped: List[str]
    norders: List[str]
    orders: List[OrderResponse]


# ─────────────────────────────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    log.info("Initializing database")
    Database.init()


# ─────────────────────────────────────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────────────────────────────────────
def extract_zip(upload: UploadFile, extract_dir: str) -> List[str]:
    zip_path = os.path.join(extract_dir, "upload.zip")

    with open(zip_path, "wb") as f:
        f.write(upload.file.read())

    try:
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(extract_dir)
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid ZIP file")

    files = [
        str(p) for p in Path(extract_dir).rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXT
    ]

    if not files:
        raise HTTPException(
            status_code=422,
            detail="ZIP contains no supported files",
        )

    return files


# ─────────────────────────────────────────────────────────────────────────────
# Main processing endpoint
# ─────────────────────────────────────────────────────────────────────────────
@app.post(
    "/api/v1/process",
    response_model=ProcessResponse,
    summary="Process ZIP and extract HVL data",
)
async def process_zip(
    file: UploadFile = File(...),
    company_id: int = Form(...),
    environment: str = Form(...),
    modifier_user: str = Form("hvl_api"),
    claude_key: str = Form(""),
    gemini_key: str = Form(""),
    openai_key: str = Form(""),
):
    if not file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP files allowed")

    if environment not in ENV_LOGIN_DB:
        raise HTTPException(status_code=400, detail="Invalid environment")

    company_db = Database.resolve_company(company_id)

    extractor = FileExtractor(
        claude_key or os.getenv("ANTHROPIC_API_KEY", ""),
        gemini_key or os.getenv("GEMINI_API_KEY", ""),
        openai_key or os.getenv("OPENAI_API_KEY", ""),
    )

    tmp = tempfile.mkdtemp(prefix="hvl_api_")
    try:
        files = extract_zip(file, tmp)

        results: List[Dict[str, Any]] = []
        skipped: List[str] = []

        for fp in files:
            name = Path(fp).name
            if any(s in name.lower() for s in SKIP_NAMES):
                skipped.append(name)
                continue

            res = extractor.process(fp)
            if res:
                results.append(res)
                log.info("Extracted %s", name)
            else:
                skipped.append(name)

        if not results:
            raise HTTPException(422, "No extractable data found")

        orders = []

        with Database.connect() as con:
            for r in results:
                item = Database.upsert_item(con, r, modifier_user)
                customer = Database.upsert_customer(con, r, modifier_user)
                order, norder = Database.insert_order(con, modifier_user, company_id)
                row = Database.insert_order_row(con, order, item, customer, modifier_user)

                Database.insert_order_values(con, row, r, modifier_user, ORDER_PARAMS)

                orders.append(
                    OrderResponse(
                        source_file=r.get("source_file"),
                        NOrder=norder,
                        IdOrder=order,
                        IdOrderRow=row,
                        IdItem=item,
                        IdCustomer=customer,
                    )
                )

        return ProcessResponse(
            status="ok",
            company_id=company_id,
            company_db=company_db,
            environment=environment,
            files_processed=len(results),
            files_skipped=skipped,
            norders=[o.NOrder for o in orders],
            orders=orders,
        )

    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# Admin endpoints
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/admin/company")
def add_company(
    company_id: int = Form(...),
    db_name: str = Form(...),
    company_name: str = Form(""),
    environment: str = Form("prod"),
):
    Database.upsert_company(company_id, db_name, company_name, environment)
    return {"status": "ok"}


@app.get("/admin/companies")
def list_companies():
    return Database.list_companies()


@app.get("/admin/stats")
def db_stats():
    return Database.stats()


@app.get("/health")
def health():
    return {"status": "ok", "version": "3.0.0"}