"""
HVL Cost Estimation — API Business Logic
Orchestrates: extraction → DB upsert → order creation → process/phase planning.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import pyodbc

from config import (
    API_USER,
    PAINTING_LINES,
    PHASE_KEYWORDS,
    PHASE_SEQUENCE,
)
from extractor import PartInfo, extract_from_zip_bytes, merge_parts
from db.connection import connect, resolve_company_db
from db.operations import (
    upsert_item,
    upsert_customer,
    insert_order,
    insert_order_row,
    insert_order_values,
    get_phases_company,
    insert_process_list,
    insert_phase_list,
)

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Painting-line selection
# ═══════════════════════════════════════════════════════════════════════════════

def select_painting_line(part: PartInfo) -> str:
    """
    Choose the best (smallest capable) painting line for a part.

    Line table (company resource):
    ┌────┬────────────────────────────┬────────┬───────┬────────┬──────────┐
    │    │ Description                │ max H  │ max L │ max P  │ max W kg │
    ├────┼────────────────────────────┼────────┼───────┼────────┼──────────┤
    │ L3 │ Linea Vernic. Automatica   │  1 500 │   500 │  1 200 │       80 │
    │ L1 │ Linea Vernic. Automatica   │  2 100 │   800 │  2 500 │      100 │
    │ L2 │ Linea Vernic. Manuale      │  2 000 │ 2 000 │  6 000 │      500 │
    └────┴────────────────────────────┴────────┴───────┴────────┴──────────┘
    Lines are tried in order L3 → L1 → L2 (most-restrictive first).
    """
    dims = sorted(filter(None, [
        part.length_mm or 0,
        part.width_mm  or 0,
        part.height_mm or 0,
    ]))
    weight    = part.weight_kg or 0
    material  = (part.material or "").upper()

    for line in PAINTING_LINES:                   # ordered L3 → L1 → L2
        max_dims = sorted([
            line["max_h_mm"],
            line["max_l_mm"],
            line["max_p_mm"],
        ])

        # Check all dimensions fit (pad dims if fewer than 3 given)
        padded = ([0] * (3 - len(dims))) + dims
        if any(d > m for d, m in zip(padded, max_dims)):
            continue

        # Check weight limit
        if weight > line["max_weight_kg"]:
            continue

        # Check material compatibility (skip check if material unknown)
        if material:
            if not any(m in material for m in line["materials"]):
                continue

        log.info(f"Selected painting line: {line['code']} for part '{part.item_code}'")
        return line["code"]

    log.warning(f"No line fit — defaulting to L2 for part '{part.item_code}'")
    return "L2"


# ═══════════════════════════════════════════════════════════════════════════════
# Phase detection
# ═══════════════════════════════════════════════════════════════════════════════

def detect_phases(text: str, phases_available: list[dict]) -> list[dict]:
    """
    Detect required production phases from document text.

    Always includes Verniciatura (painting is the core service).
    Automatically adds Preparazione when painting is detected.

    Returns list of phase dicts sorted by PHASE_SEQUENCE, each dict:
      {id, id_phase, phase_code, description}
    """
    if not text:
        text = ""
    text_lower = text.lower()

    detected: set[str] = set()

    # Core service: painting is always present (this is a painting shop)
    detected.add("V001")   # Verniciatura

    # Preparation always precedes painting
    detected.add("P001")   # Preparazione

    # Scan remaining phases for keywords
    for code, keywords in PHASE_KEYWORDS.items():
        if code in detected:
            continue
        for kw in keywords:
            if kw in text_lower:
                detected.add(code)
                break

    # Map phase code → PhasesCompany record
    phase_map: dict[str, dict] = {p["phase_code"]: p for p in phases_available}

    # Order by logical sequence
    result: list[dict] = []
    for code in PHASE_SEQUENCE:
        if code in detected and code in phase_map:
            result.append(phase_map[code])

    # Append any detected phases not in the canonical sequence
    seq_set = set(PHASE_SEQUENCE)
    for code in detected:
        if code not in seq_set and code in phase_map:
            result.append(phase_map[code])

    log.info(f"Detected phases: {[p['phase_code'] for p in result]}")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Per-part pipeline
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PartResult:
    item_code:    str
    item_id:      int
    customer_id:  int
    order_row_id: int
    surface_area: Optional[float]
    painting_line: str
    phases:       list[str]
    error:        Optional[str] = None


def _process_part(conn: pyodbc.Connection,
                  part: PartInfo,
                  order_id: int,
                  phases_available: list[dict]) -> PartResult:
    """Full pipeline for a single extracted part."""

    # ── 1. Item ───────────────────────────────────────────────────────────────
    code = part.item_code or "UNKNOWN"
    item_id = upsert_item(conn, code, part.item_description)

    # ── 2. Customer ───────────────────────────────────────────────────────────
    cust_id = upsert_customer(
        conn,
        name      = part.customer_name,
        email     = part.customer_email,
    )

    # ── 3. Painting line (param 34) ───────────────────────────────────────────
    line = select_painting_line(part)

    # ── 4. Surface area (param 33) ────────────────────────────────────────────
    area = part.surface_area_m2
    if not area:
        area = part.calculate_surface_area()

    # ── 5. Order row ──────────────────────────────────────────────────────────
    row_id = insert_order_row(conn, order_id, item_id, cust_id)

    # ── 6. Order values ───────────────────────────────────────────────────────
    insert_order_values(
        conn,
        order_row_id           = row_id,
        ral_color              = part.ral_color,
        finishing_type         = part.finishing_type,
        batch_size             = part.batch_size,
        pitch_mm               = part.pitch_mm,
        has_protections        = part.has_protections,
        protection_description = part.protection_description,
        surface_area_m2        = area,
        painting_line          = line,
    )

    # ── 7. Process list (one per order row) ───────────────────────────────────
    proc_id = insert_process_list(conn, row_id, item_id, code)

    # ── 8. Phase detection + phase list ───────────────────────────────────────
    raw_text = part.raw_text or ""
    phases = detect_phases(raw_text, phases_available)
    detected_codes: list[str] = []

    for seq_num, phase in enumerate(phases, start=1):
        insert_phase_list(conn, proc_id, phase["id"], sequence=seq_num)
        detected_codes.append(phase["phase_code"])

    log.info(f"✓ Part '{code}': row={row_id}, area={area}, line={line}, phases={detected_codes}")

    return PartResult(
        item_code     = code,
        item_id       = item_id,
        customer_id   = cust_id,
        order_row_id  = row_id,
        surface_area  = area,
        painting_line = line,
        phases        = detected_codes,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Public entry point
# ═══════════════════════════════════════════════════════════════════════════════

def process_zip(zip_bytes: bytes,
                company_id: int,
                environment: str) -> dict:
    """
    Full pipeline:
      1. Resolve company DB
      2. Extract parts from ZIP
      3. Merge / deduplicate data
      4. For each part: upsert Item+Customer, create OrderRow, OrderValues,
         ProcessList, PhasesList
      5. Return summary with NOrder

    Returns dict with keys:
      norder, order_id, environment, company_id, company_db, parts
    """
    # ── DB setup ──────────────────────────────────────────────────────────────
    company_db = resolve_company_db(company_id, environment)
    conn = connect(company_db)

    # ── Extraction ────────────────────────────────────────────────────────────
    raw_parts = extract_from_zip_bytes(zip_bytes)
    if not raw_parts:
        raise ValueError("No supported files (PDF/EML/MSG) found in the ZIP archive.")

    parts = merge_parts(raw_parts)
    log.info(f"Processing {len(parts)} part(s) from ZIP")

    # ── Phases reference data ─────────────────────────────────────────────────
    phases_available = get_phases_company(conn)
    log.info(f"PhasesCompany: {len(phases_available)} phases available")

    # ── Create order (one per API call) ──────────────────────────────────────
    note = f"Auto-import from ZIP: {len(parts)} part(s)"
    order_id, norder = insert_order(conn, note=note)

    # ── Process each part ─────────────────────────────────────────────────────
    results: list[dict] = []
    errors:  list[str]  = []

    for part in parts:
        try:
            pr = _process_part(conn, part, order_id, phases_available)
            results.append({
                "item_code":    pr.item_code,
                "item_id":      pr.item_id,
                "order_row_id": pr.order_row_id,
                "surface_area": pr.surface_area,
                "painting_line":pr.painting_line,
                "phases":       pr.phases,
            })
        except Exception as exc:
            log.exception(f"Failed to process part '{part.item_code}': {exc}")
            errors.append(f"{part.item_code}: {exc}")

    conn.close()

    return {
        "norder":      norder,
        "order_id":    order_id,
        "environment": environment,
        "company_id":  company_id,
        "company_db":  company_db,
        "parts":       results,
        "errors":      errors,
    }
