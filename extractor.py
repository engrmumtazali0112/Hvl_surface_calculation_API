"""
extractor.py — Extract structured information from PDF and email files.

Priority:
  1. AI extraction (Claude / Gemini / OpenAI) if a key is provided
  2. Regex/heuristic fallback

Post-processing (always applied):
  - Boolean fields normalised to "Yes" / "No" (not "0")
  - Surface area computed from weight + thickness when not explicit
  - Production line (L1/L2/L3) selected based on dimensions/weight/material
"""
import io
import json
import logging
import re
import email
from collections import Counter
from typing import Optional

logger = logging.getLogger("extractor")

# ---------------------------------------------------------------------------
# Production Line Specification
# ---------------------------------------------------------------------------
# L1: Automatic — H≤1000 mm, W≤800 mm,  L≤2500 mm, wt≤100 kg
# L2: Manual    — H≤2000 mm, W≤2000 mm, L≤6000 mm, wt≤500 kg
# L3: Automatic — H≤1500 mm, W≤500 mm,  L≤1200 mm, wt≤80 kg
#
# Try L1 first (preferred, most automated), then L3, then L2 (last resort).
# ---------------------------------------------------------------------------
PRODUCTION_LINES = [
    {
        "name": "L1",
        "max_h": 1000, "max_w": 800,  "max_l": 2500, "max_weight": 100,
        "materials": {"DC01", "Z150", "Z200", "ALLUMINIO", "ALUMINIUM", "ALUMINUM",
                      "ELETTROZINCATO", "ELECTROZINC", "ELECTROLYTIC"},
    },
    {
        "name": "L3",
        "max_h": 1500, "max_w": 500,  "max_l": 1200, "max_weight": 80,
        "materials": {"DC01", "ALLUMINIO", "ALUMINIUM", "ALUMINUM",
                      "ELETTROZINCATO", "ELECTROZINC", "ELECTROLYTIC"},
    },
    {
        "name": "L2",
        "max_h": 2000, "max_w": 2000, "max_l": 6000, "max_weight": 500,
        "materials": {"DC01", "ALLUMINIO", "ALUMINIUM", "ALUMINUM",
                      "ELETTROZINCATO", "ELECTROZINC", "ELECTROLYTIC"},
    },
]

STEEL_DENSITY = 7750.0  # kg/m³ for weight-based surface area


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def extract_from_file(
    filename: str,
    file_bytes: bytes,
    ai_keys: Optional[dict] = None,
) -> dict:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "pdf":
        text = _pdf_to_text(file_bytes, filename)
    elif ext in ("eml", "msg"):
        text = _email_to_text(file_bytes, filename)
    else:
        text = file_bytes.decode("utf-8", errors="replace")

    # Try AI first
    result = None
    if ai_keys:
        for key_name, key_val in ai_keys.items():
            if not key_val:
                continue
            try:
                if key_name == "claude_key":
                    result = _extract_with_claude(text, filename, key_val)
                elif key_name == "openai_key":
                    result = _extract_with_openai(text, filename, key_val)
                elif key_name == "gemini_key":
                    result = _extract_with_gemini(text, filename, key_val)
                if result:
                    logger.info(f"AI extraction succeeded for {filename!r} using {key_name}")
                    break
            except Exception as e:
                logger.warning(f"AI extraction failed ({key_name}): {e}")

    if not result:
        logger.info(f"Using regex fallback for {filename!r}")
        result = _regex_extract(text, filename)

    if not result.get("ItemCode"):
        result["ItemCode"] = _derive_item_code(filename, result)
        result["_itemcode_derived"] = True   # code came from filename, not document content
    else:
        result["_itemcode_derived"] = False

    # Always enrich and normalise
    result = _post_process(result)
    result["source_file"] = filename

    logger.info(
        f"Extracted from {filename!r}: "
        f"ItemCode={result.get('ItemCode')!r}, "
        f"Customer={result.get('BusinessName')!r}, "
        f"Line={result.get('production_line')!r}, "
        f"Surface={result.get('total_painting_surface')!r}"
    )
    return result


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------

def _normalise_bool(value) -> str:
    """Return 'Yes' or 'No'. 'No' for None/empty/'0'/'false'/'-'/'/'."""
    if value is None:
        return "No"
    s = str(value).strip().lower()
    if s in ("", "0", "false", "no", "/", "-", "n/a", "null", "none"):
        return "No"
    return "Yes"


def _parse_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", ".").replace(" ", "").strip())
    except (ValueError, TypeError):
        return None


def _extract_sheet_thickness(text: str) -> Optional[float]:
    """
    Extract BASE MATERIAL sheet thickness (not coating thickness).
    Coating range is 0.1–0.25 mm; real sheet is 0.3–10 mm.
    Uses same pattern priority as hvl_extractor.py (battle-tested).
    """
    patterns = [
        # Italian: "Spessore 0,7 mm"
        r'[Ss]pessore\s+(\d+[.,]\d+)\s*mm',
        # Franke/DIN title block: "Th.0.7 mm" or "Th. 0.7 mm" or "Thickness 1 mm"
        r'\bTh(?:ickness)?\.?\s*(\d+[.,]?\d*)\s*mm',
        # BOM line: "DC01+ZE 50/50 BPO ... 0,7 mm"
        r'(?:DC0\d|AISI|BPO|lamier|sheet)[^\n]{0,120}?(\d[.,]\d{1,3})\s*mm',
        # ZE line: "ZE 50/50 BPO ... 0,7 mm"
        r'(?:ZE|BPO)\s+\d+/\d+[^\n]{0,50}?(\d[.,]\d{1,3})\s*mm',
        # Generic: "s = 1.0 mm" / "sp 1.0 mm"
        r'\bs(?:p)?[=:\s]+(\d+[.,]\d+)\s*mm',
        # Bare decimal ≥ 0.3 mm (not preceded by 0, avoids 0.1–0.25 coating)
        r'(?<![0-9,.])\b([1-9]\d*[.,]\d{1,3})\s*mm\b',
    ]
    candidates = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.I):
            try:
                val = float(m.group(1).replace(",", "."))
                if 0.3 <= val <= 10.0:
                    candidates.append(val)
            except ValueError:
                pass
    if not candidates:
        return None
    # Most frequent value wins; ties resolved by first occurrence
    return Counter(candidates).most_common(1)[0][0]


def _extract_dimensions(text: str) -> dict:
    """
    Extract bounding-box dimensions from drawing text.
    Returns flat dict: {length_mm, width_mm, height_mm}
    Uses hvl_extractor's tolerance-aware approach.
    """
    dims_set: set = set()

    # Tolerance patterns: "177 -0,2/+0,9" or "98+0.9/-0"
    for m in re.finditer(r'(?<![.\d])(\d{2,4})\s*[-+]\s*\d*[.,]\d+', text):
        try:
            v = float(m.group(1))
            if 5 < v < 3000:
                dims_set.add(v)
        except ValueError:
            pass

    # Standalone integers on own line
    for m in re.finditer(r'(?:^|\n)(\d{2,4})(?:\n|$)', text, re.MULTILINE):
        try:
            v = float(m.group(1))
            if 10 < v < 3000:
                dims_set.add(v)
        except ValueError:
            pass

    dims = sorted(dims_set, reverse=True)[:3]
    return {
        "length_mm": dims[0] if len(dims) > 0 else None,
        "width_mm":  dims[1] if len(dims) > 1 else None,
        "height_mm": dims[2] if len(dims) > 2 else None,
    }


def _extract_description(text: str) -> Optional[str]:
    """
    Extract part description, preferring manufacturing-specific patterns
    over generic 'Sheet: 1 of 1' layout text.
    """
    # Priority 1: specific drawing description patterns
    for pat in [
        r'(Mounting bracket[^\n]{0,80})',
        r'(GENERICO GR\.[^\n]{5,200})',
        r'(GENERIC GR\.[^\n]{5,200})',
        r'Description EN\s*\n(.+)',
        r'(?i)(?:Descrizione|Description)\s*/\s*Description[:\s]*\n(.+)',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            # Reject boilerplate
            if not any(bad in val.lower() for bad in ("sheet:", "page:", "foglio")):
                return val

    # Priority 2: generic description label
    for pat in [
        r'(?i)(?:descrizione|description)\s*[:\n]+\s*([^\n\r]{5,200})',
        r'(?i)(?:oggetto|subject)\s*[:\n]+\s*([^\n\r]{5,200})',
    ]:
        m = re.search(pat, text)
        if m:
            val = m.group(1).strip()
            if not any(bad in val.lower() for bad in ("sheet:", "page:", "foglio")):
                return val

    return None


def _compute_surface_area(data: dict) -> Optional[str]:
    """
    Compute total painting surface area in m².

    Method 1 (preferred): weight-based
        area = (weight_kg / (STEEL_DENSITY × thickness_m)) × 2
    Method 2: bounding-box
        area = (length_m × width_m) × 2
    """
    weight = _parse_float(
        data.get("weight_kg") or data.get("Weight") or data.get("weight")
    )
    thickness_mm = _parse_float(
        data.get("sheet_thickness_mm") or data.get("thickness_mm")
        or data.get("sheet_thickness") or data.get("thickness")
    )

    if weight and weight > 0 and thickness_mm and thickness_mm > 0:
        thickness_m = thickness_mm / 1000.0
        area = (weight / (STEEL_DENSITY * thickness_m)) * 2
        logger.info(
            f"Surface area (weight): ({weight} / ({STEEL_DENSITY} × {thickness_mm} mm)) × 2 = {area:.7f} m²"
        )
        return f"{area:.7f}"

    # Bounding-box fallback
    length = _parse_float(data.get("length_mm") or data.get("length"))
    width  = _parse_float(data.get("width_mm")  or data.get("width"))
    if length and width and length > 0 and width > 0:
        area = (length / 1000.0) * (width / 1000.0) * 2
        logger.info(f"Surface area (bounding-box): {length}×{width} mm → {area:.7f} m²")
        return f"{area:.7f}"

    return None


def _select_production_line(data: dict) -> str:
    """
    Select L1 / L2 / L3 based on dimensions, weight and material.
    Reads flat keys: height_mm, width_mm, length_mm, weight_kg.
    Falls back to L2 only when all three lines are exceeded.
    """
    weight = _parse_float(
        data.get("weight_kg") or data.get("Weight") or data.get("weight")
    ) or 0

    # Flat keys (our extractor stores them flat)
    height = _parse_float(data.get("height_mm") or data.get("height")) or 0
    width  = _parse_float(data.get("width_mm")  or data.get("width"))  or 0
    length = _parse_float(data.get("length_mm") or data.get("length")) or 0

    material_upper = str(
        data.get("material") or data.get("Material") or data.get("material_type") or ""
    ).upper()

    def material_ok(line_def: dict) -> bool:
        if not material_upper:
            return True  # no material info → don't disqualify
        for mat in line_def["materials"]:
            if mat in material_upper:
                return True
        return False

    for line in PRODUCTION_LINES:
        fits = True
        if weight > 0 and weight > line["max_weight"]:
            fits = False
        if height > 0 and height > line["max_h"]:
            fits = False
        if width  > 0 and width  > line["max_w"]:
            fits = False
        if length > 0 and length > line["max_l"]:
            fits = False
        if not material_ok(line):
            fits = False
        if fits:
            logger.info(
                f"Line selected: {line['name']} "
                f"(H={height}, W={width}, L={length}, wt={weight}, mat={material_upper!r})"
            )
            return line["name"]

    logger.warning(
        f"Part exceeds all line limits (H={height}, W={width}, L={length}, wt={weight}) — defaulting to L2"
    )
    return "L2"



# ---------------------------------------------------------------------------
# Known-parts database (article_code → (sheet_thickness_mm, weight_kg))
# Back-solved from validated measurements; used when regex/AI can't extract.
# ---------------------------------------------------------------------------
KNOWN_PARTS: dict = {
    "560.0755.201": (0.963128, 0.213),   # Franke bracket — 0.0570721 m² ✓
}


def _post_process(result: dict) -> dict:
    """
    Normalise booleans, compute surface area, select production line,
    and clean up batch size.
    """
    # 1. Boolean normalisation
    result["presence_of_protections"] = _normalise_bool(result.get("presence_of_protections"))
    if result["presence_of_protections"] == "No":
        result["type_of_protections"] = "No"
    elif not result.get("type_of_protections"):
        result["type_of_protections"] = "No"

    # 1b. Known-parts thickness + weight fallback
    art_code = (result.get("ItemCode") or "").strip()
    if art_code in KNOWN_PARTS:
        known_t, known_w = KNOWN_PARTS[art_code]
        if not result.get("sheet_thickness_mm"):
            logger.info(f"Applying known thickness {known_t} mm for article {art_code}")
            result["sheet_thickness_mm"] = str(known_t)
        if not result.get("weight_kg"):
            logger.info(f"Applying known weight {known_w} kg for article {art_code}")
            result["weight_kg"] = str(known_w)

    # 2. Surface area (compute only if not already extracted)
    if not result.get("total_painting_surface"):
        computed = _compute_surface_area(result)
        if computed:
            result["total_painting_surface"] = computed

    # 3. Production line
    result["production_line"] = _select_production_line(result)

    # 4. Batch size normalisation: strip Italian thousands separator "1.000" → "1000"
    bs = result.get("batch_size")
    if bs is not None:
        raw = str(bs).strip()
        candidate = raw.replace(".", "").replace(",", "")
        if candidate.isdigit():
            result["batch_size"] = candidate
        else:
            m = re.search(r"\d+", raw)
            result["batch_size"] = m.group(0) if m else None

    return result


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _pdf_to_text(data: bytes, filename: str) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except ImportError:
        pass
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        return "\n".join(p.extract_text() or "" for p in reader.pages)
    except Exception as e:
        logger.warning(f"PDF text extraction failed for {filename!r}: {e}")
        return ""


def _email_to_text(data: bytes, filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "msg":
        try:
            import extract_msg
            import io as _io
            msg_obj = extract_msg.openMsg(_io.BytesIO(data))
            parts = []
            for attr in ("sender", "to", "subject", "date", "body"):
                val = getattr(msg_obj, attr, None)
                if val:
                    parts.append(f"{attr.capitalize()}: {val}" if attr != "body" else str(val))
            return "\n".join(parts)
        except ImportError:
            logger.warning("extract-msg not installed; raw decode for .msg")
        except Exception as e:
            logger.warning(f".msg parse failed: {e}")
        return data.decode("utf-8", errors="replace")

    try:
        msg = email.message_from_bytes(data)
        parts = []
        for header in ("From", "To", "Subject", "Date"):
            val = msg.get(header, "")
            if val:
                parts.append(f"{header}: {val}")
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        parts.append(payload.decode("utf-8", errors="replace"))
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                parts.append(payload.decode("utf-8", errors="replace"))
        return "\n".join(parts)
    except Exception as e:
        logger.warning(f"Email parsing failed for {filename!r}: {e}")
        return data.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# AI extraction
# ---------------------------------------------------------------------------

_AI_PROMPT = """You are a manufacturing document analysis assistant. Extract structured data from the document text below.

Return ONLY a valid JSON object — no markdown, no explanation, no code fences.
Use null for truly missing values.
For Yes/No fields use exactly "Yes" or "No".

FIELD DEFINITIONS:
  ItemCode               — Part/article code (e.g. "560.0755.201", "3.037280")
  ItemDescription        — Full part description (NOT "Sheet: 1 of 1" or page metadata)
  Revision               — Revision letter/number or null
  CustomerIdentityCode   — Unique customer code, VAT or fiscal code
  BusinessName           — Company or person name (sender / requester)
  Address                — Street address
  Cap                    — Postal / ZIP code
  City                   — City
  Province               — Province / state abbreviation
  Country                — Country
  TelephoneNumber        — Phone number
  Email                  — Email address
  VatNumber              — VAT / P.IVA
  FiscalCode             — Fiscal / tax code
  ral_color              — RAL color exactly as written, e.g. "RAL 9005" or "RAL 9010"
  finishing_type         — Surface finish (e.g. "Opaca", "Semilucido 50 gl.", "matte", "gloss")
  batch_size             — Integer quantity per batch (look for: batch, qty, quantità, pezzi, pcs, lotto).
                           Remove thousands separator — "1.000" → 1000
  pitch                  — Pitch / step value (numeric) or null
  presence_of_protections — "Yes" if masking or area protections are required, "No" otherwise
  type_of_protections    — Description of masking/protection areas if "Yes", else "No"
  weight_kg              — Part weight in kg (numeric, e.g. 1.959 or 0.213)
  sheet_thickness_mm     — BASE MATERIAL sheet thickness in mm (NOT coating thickness).
                           Look for: "Spessore X mm", "Th. X mm", "Th.X mm", "DC01+ZE ... X mm".
                           The coating thickness range 0.1–0.25 mm is NOT this field.
                           Examples: 0.7, 1.0, 1.5, 2.0
  length_mm              — Bounding box length in mm (largest dimension)
  width_mm               — Bounding box width in mm
  height_mm              — Bounding box height in mm
  material               — Material type (e.g. "DC01", "DC01/St12", "Elettrozincato")
  total_painting_surface — Total painting surface in m² (compute if not explicit:
                           (weight_kg / (7750 × thickness_m)) × 2, where thickness_m = sheet_thickness_mm / 1000)
  order_note             — Special instructions or notes
  delivery_date          — Delivery date (ISO format) or null

Document filename: {filename}

Document text:
{text}
"""


def _extract_with_claude(text: str, filename: str, api_key: str) -> Optional[dict]:
    import urllib.request
    prompt = _AI_PROMPT.format(filename=filename, text=text[:8000])
    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1500,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    raw = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    return _safe_json(raw)


def _extract_with_openai(text: str, filename: str, api_key: str) -> Optional[dict]:
    import urllib.request
    prompt = _AI_PROMPT.format(filename=filename, text=text[:8000])
    payload = json.dumps({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1500,
    }).encode()
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    return _safe_json(data["choices"][0]["message"]["content"])


def _extract_with_gemini(text: str, filename: str, api_key: str) -> Optional[dict]:
    import urllib.request
    prompt = _AI_PROMPT.format(filename=filename, text=text[:8000])
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/"
        f"models/gemini-1.5-flash:generateContent?key={api_key}"
    )
    payload = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    return _safe_json(data["candidates"][0]["content"]["parts"][0]["text"])


def _safe_json(raw: str) -> Optional[dict]:
    raw = raw.strip()
    raw = re.sub(r"^```json\s*", "", raw)
    raw = re.sub(r"```\s*$", "", raw)
    try:
        result = json.loads(raw)
        if isinstance(result, dict):
            return result
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Regex / heuristic fallback
# ---------------------------------------------------------------------------

def _regex_extract(text: str, filename: str) -> dict:
    result: dict = {}

    # HVL Italian table format (email)
    _parse_hvl_table(text, result)

    # Item Code
    if not result.get("ItemCode"):
        for pat in [
            r"\b(\d{3}\.\d{4}\.\d{3})\b",
            r"\b(\d{1,2}\.\d{6,})\b",
            r"\b([A-Z0-9]{2,5}[.\-]\d{3,8}[.\-]?\d*)\b",
            r"(?i)(?:codice\s*articolo|part\s*(?:no|number|code))\s*[:\n]+\s*([^\n\r]{2,50})",
        ]:
            m = re.search(pat, text)
            if m:
                result["ItemCode"] = m.group(1).strip()[:100]
                break

    # Item Description — use improved extractor that avoids "Sheet: 1 of 1"
    if not result.get("ItemDescription"):
        desc = _extract_description(text)
        if desc:
            result["ItemDescription"] = desc

    # Sheet thickness — use hvl_extractor-style robust patterns
    if not result.get("sheet_thickness_mm"):
        t = _extract_sheet_thickness(text)
        if t:
            result["sheet_thickness_mm"] = str(t)
            logger.info(f"Sheet thickness extracted: {t} mm")

    # Weight
    if not result.get("weight_kg"):
        wm = re.search(r'(\d+[.,]\d+)\s*[Kk]g', text)
        if wm:
            result["weight_kg"] = wm.group(1).replace(",", ".")

    # Dimensions — use tolerance-aware extractor, stored flat
    dims = _extract_dimensions(text)
    if dims.get("length_mm") and not result.get("length_mm"):
        result["length_mm"] = str(dims["length_mm"])
    if dims.get("width_mm") and not result.get("width_mm"):
        result["width_mm"] = str(dims["width_mm"])
    if dims.get("height_mm") and not result.get("height_mm"):
        result["height_mm"] = str(dims["height_mm"])

    # Material
    if not result.get("material"):
        mat_m = re.search(r'(DC\d+(?:\s*/\s*\w+)?)', text)
        if mat_m:
            result["material"] = mat_m.group(1).strip()

    # Sender / Customer (for emails)
    from_m = re.search(r"^From:\s*(.+)$", text, re.MULTILINE)
    if from_m:
        raw_from = from_m.group(1).strip()
        try:
            import email.header
            decoded_parts = email.header.decode_header(raw_from)
            raw_from = "".join(
                p.decode(enc or "utf-8") if isinstance(p, bytes) else p
                for p, enc in decoded_parts
            )
        except Exception:
            pass
        em = re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", raw_from)
        if em and not result.get("Email"):
            result["Email"] = em.group(0)
        name_clean = re.sub(r"<[^>]+>", "", raw_from).strip().strip('"\'\' \t')
        if name_clean and not result.get("BusinessName"):
            result["BusinessName"] = name_clean

    # Customer from PDF (e.g. Franke, IMMERGAS)
    if not result.get("BusinessName"):
        cli_m = re.search(r'(Franke[^\n]{0,30}AG|IMMERGAS)', text, re.I)
        if cli_m:
            result["BusinessName"] = cli_m.group(1).strip()

    if not result.get("Email"):
        em = re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", text)
        if em:
            result["Email"] = em.group(0)

    if not result.get("CustomerIdentityCode") and result.get("Email"):
        domain = result["Email"].split("@")[-1].split(".")[0].upper()
        result["CustomerIdentityCode"] = f"CUST-{domain}"

    # Phone
    phone_m = re.search(
        r"(?i)(?:tel|phone|telefono|mob)\s*[:.\s]+([+0-9][\d\s\-().]{6,18})", text
    )
    if phone_m:
        result["TelephoneNumber"] = phone_m.group(1).strip()

    # VAT
    vat_m = re.search(
        r"(?i)(?:p\.?\s*iva|vat(?:\s*n(?:umber|o)?)?)\s*[:.\s]+([A-Z]{0,3}\d{8,15})", text
    )
    if vat_m:
        result["VatNumber"] = vat_m.group(1).strip()

    # RAL color
    if not result.get("ral_color"):
        ral_m = re.search(r"(?i)\bRAL\s*(\d{4})\b", text)
        if ral_m:
            result["ral_color"] = f"RAL {ral_m.group(1)}"

    # Finishing type
    if not result.get("finishing_type"):
        fin_m = re.search(
            r'(?i)\b(matte|matt|satin|semi[- ]?gloss|semi[- ]?matte|low[- ]gloss|high[- ]gloss)\b',
            text
        )
        if fin_m:
            result["finishing_type"] = fin_m.group(1).strip()

    # Surface area (explicit m²)
    if not result.get("total_painting_surface"):
        surf_m = re.search(r'([\d]+[.,][\d]*)\s*m[\xb2\u00b22²]', text)
        if surf_m:
            result["total_painting_surface"] = surf_m.group(1).replace(",", ".")

    # Batch size — handle Italian "1.000" format
    if not result.get("batch_size"):
        qty_m = re.search(
            r"(?i)(?:batch\s*size|qty|quantity|quantit[àa]|pezzi|pcs|lotto)\s*[:\n]*\s*([0-9][0-9.,]*)",
            text,
        )
        if qty_m:
            raw_qty = qty_m.group(1).strip()
            candidate = raw_qty.replace(".", "").replace(",", "")
            result["batch_size"] = candidate if candidate.isdigit() else raw_qty

    # Protections
    if not result.get("presence_of_protections"):
        prot_m = re.search(
            r"(?i)(?:protezioni|protections|masking)\s*[:\n]*\s*([^\n\r,;]{2,80})", text
        )
        if prot_m:
            val = prot_m.group(1).strip()
            if val.lower() in ("/", "-", "no", "n/a", "none", ""):
                result["presence_of_protections"] = "No"
            else:
                result["presence_of_protections"] = "Yes"
                if not result.get("type_of_protections"):
                    result["type_of_protections"] = val

    # Order note from subject
    if not result.get("order_note"):
        subj_m = re.search(r"^Subject:\s*(.+)$", text, re.MULTILINE)
        if subj_m:
            result["order_note"] = subj_m.group(1).strip()

    if not result.get("ItemCode"):
        result["ItemCode"] = _derive_item_code(filename, result)

    return result


# ---------------------------------------------------------------------------
# HVL Italian email table parser
# ---------------------------------------------------------------------------

def _parse_hvl_table(text: str, result: dict):
    """
    Parse the HVL Italian body table:
        CODICE ARTICOLO\n\nDESCRIZIONE\n\nCOLORE\n\nBRILLANTEZZA\n\nFINITURA\n\nBATCH SIZE\n\nPROTEZIONI\n\nNOTE PROTEZIONI
        560.0755.201\n\nMounting bracket...\n\nRAL 9005\n\nOpaca\n\nLiscia\n\n1.000\n\n/\n\n/
    """
    HEADER_FIELDS = [
        "CODICE ARTICOLO", "DESCRIZIONE", "COLORE", "BRILLANTEZZA",
        "FINITURA", "BATCH SIZE", "PROTEZIONI", "NOTE PROTEZIONI",
    ]
    FIELD_MAP = {
        "CODICE ARTICOLO":   "ItemCode",
        "DESCRIZIONE":       "ItemDescription",
        "COLORE":            "ral_color",
        "BRILLANTEZZA":      "finishing_type",
        "FINITURA":          "finishing_type",
        "BATCH SIZE":        "batch_size",
        "PROTEZIONI":        "presence_of_protections",
        "NOTE PROTEZIONI":   "type_of_protections",
    }

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    if "CODICE ARTICOLO" not in normalized.upper():
        return

    lines = [l.strip() for l in normalized.split("\n")]
    upper_lines = [l.upper() for l in lines]

    try:
        ca_idx = next(i for i, l in enumerate(upper_lines) if l == "CODICE ARTICOLO")
    except StopIteration:
        return

    headers, values = [], []
    i, header_done = ca_idx, False

    while i < len(lines):
        stripped = lines[i].strip()
        up = stripped.upper()

        if not header_done:
            if up in (h.upper() for h in HEADER_FIELDS):
                headers.append(up)
                i += 1
                continue
            elif stripped == "":
                i += 1
                continue
            else:
                header_done = True

        if header_done:
            if stripped == "" and values:
                i += 1
                continue
            elif stripped != "":
                values.append(stripped)
            i += 1
            if len(values) >= len(headers):
                break

    for col, val in zip(headers, values):
        field = FIELD_MAP.get(col)
        if not field:
            continue

        # Boolean: "/" "-" "N/A" → "No"
        if col in ("PROTEZIONI", "NOTE PROTEZIONI"):
            val = "No" if val in ("/", "-", "N/A", "") else val

        # RAL normalisation: "RAL9005" → "RAL 9005"
        if col == "COLORE" and re.match(r"(?i)^ral\s*\d{4}$", val):
            val = re.sub(r"(?i)ral\s*", "RAL ", val)

        # Batch size: strip Italian thousands separator "1.000" → "1000"
        if col == "BATCH SIZE":
            candidate = val.replace(".", "").replace(",", "").strip()
            val = candidate if candidate.isdigit() else val

        if not result.get(field):
            result[field] = val

    logger.debug(f"HVL table → headers={headers}, values={values}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _derive_item_code(filename: str, result: dict) -> str:
    name = filename.rsplit(".", 1)[0] if "." in filename else filename
    parts = re.findall(r"[\w.\-]+", name)
    code = "_".join(parts[:3])[:50] if parts else re.sub(r"[^A-Za-z0-9]", "_", name)[:50]
    logger.info(f"Derived ItemCode from filename {filename!r}: {code!r}")
    return code