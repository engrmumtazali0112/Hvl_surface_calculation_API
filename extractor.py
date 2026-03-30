"""
extractor.py — Extract structured information from PDF and email files.

Priority:
  1. AI extraction (Claude / Gemini / OpenAI) if a key is provided
  2. Regex/heuristic fallback

Returns a dict with keys matching db/operations.py expectations:
  ItemCode, ItemDescription, Revision,
  CustomerIdentityCode, BusinessName, Address, Cap, City, Province,
  Country, TelephoneNumber, Email, VatNumber, FiscalCode,
  ral_color, finishing_type, batch_size, pitch,
  presence_of_protections, type_of_protections, total_painting_surface,
  order_note, delivery_date
"""
import io
import json
import logging
import re
import email
from typing import Optional

logger = logging.getLogger("extractor")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def extract_from_file(
    filename: str,
    file_bytes: bytes,
    ai_keys: Optional[dict] = None,
) -> dict:
    """
    Dispatch extraction based on file extension.
    Returns a unified extraction dict.
    """
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

    # Always ensure ItemCode is populated
    if not result.get("ItemCode"):
        result["ItemCode"] = _derive_item_code(filename, result)

    logger.info(f"Extracted from {filename!r}: ItemCode={result.get('ItemCode')!r}, "
                f"Customer={result.get('BusinessName')!r}")
    return result


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _pdf_to_text(data: bytes, filename: str) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n".join(pages)
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

    # .msg is Outlook binary format — try extract-msg library first
    if ext == "msg":
        try:
            import extract_msg  # pip install extract-msg
            import io as _io
            msg_obj = extract_msg.openMsg(_io.BytesIO(data))
            parts = []
            if msg_obj.sender:
                parts.append(f"From: {msg_obj.sender}")
            if msg_obj.to:
                parts.append(f"To: {msg_obj.to}")
            if msg_obj.subject:
                parts.append(f"Subject: {msg_obj.subject}")
            if msg_obj.date:
                parts.append(f"Date: {msg_obj.date}")
            if msg_obj.body:
                parts.append(msg_obj.body)
            return "\n".join(parts)
        except ImportError:
            logger.warning("extract-msg not installed; falling back to raw decode for .msg")
        except Exception as e:
            logger.warning(f".msg parse failed ({e}); falling back to raw decode")
        # Last resort: decode raw bytes (may contain some readable text)
        return data.decode("utf-8", errors="replace")

    # .eml — standard RFC 822
    try:
        msg = email.message_from_bytes(data)
        parts = []

        # Headers
        for header in ("From", "To", "Subject", "Date"):
            val = msg.get(header, "")
            if val:
                parts.append(f"{header}: {val}")

        # Body
        if msg.is_multipart():
            for part in msg.walk():
                ct = part.get_content_type()
                if ct == "text/plain":
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

_AI_PROMPT = """You are a document analysis assistant. Extract structured information from the following document text.

Return ONLY a valid JSON object (no markdown, no explanation) with these fields (use null for missing values):
{
  "ItemCode": "product/part code or order reference",
  "ItemDescription": "product description",
  "Revision": "revision or version",
  "CustomerIdentityCode": "customer unique code or VAT/fiscal code",
  "BusinessName": "company or person name",
  "Address": "street address",
  "Cap": "postal/zip code",
  "City": "city",
  "Province": "province or state abbreviation",
  "Country": "country",
  "TelephoneNumber": "phone number",
  "Email": "email address",
  "VatNumber": "VAT number",
  "FiscalCode": "fiscal/tax code",
  "ral_color": "RAL color code or name",
  "finishing_type": "surface finishing type",
  "batch_size": "quantity or batch size",
  "pitch": "pitch value",
  "presence_of_protections": "yes/no or description of protections",
  "type_of_protections": "type of protections",
  "total_painting_surface": "total painting surface area in m2",
  "order_note": "any additional notes or special instructions",
  "delivery_date": "requested delivery date (ISO format if possible)"
}

Document filename: {filename}

Document text:
{text}
"""


def _extract_with_claude(text: str, filename: str, api_key: str) -> Optional[dict]:
    import urllib.request
    prompt = _AI_PROMPT.format(filename=filename, text=text[:8000])
    payload = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1000,
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

    content = data.get("content", [])
    raw = "".join(b.get("text", "") for b in content if b.get("type") == "text")
    return _safe_json(raw)


def _extract_with_openai(text: str, filename: str, api_key: str) -> Optional[dict]:
    import urllib.request
    prompt = _AI_PROMPT.format(filename=filename, text=text[:8000])
    payload = json.dumps({
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1000,
    }).encode()

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    raw = data["choices"][0]["message"]["content"]
    return _safe_json(raw)


def _extract_with_gemini(text: str, filename: str, api_key: str) -> Optional[dict]:
    import urllib.request
    prompt = _AI_PROMPT.format(filename=filename, text=text[:8000])
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/"
        f"models/gemini-1.5-flash:generateContent?key={api_key}"
    )
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}]
    }).encode()

    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    raw = data["candidates"][0]["content"]["parts"][0]["text"]
    return _safe_json(raw)


def _safe_json(raw: str) -> Optional[dict]:
    """Strip markdown fences and parse JSON."""
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

    # ── HVL Italian table format ──────────────────────────────────────────
    # The EML body has rows like:
    #   CODICE ARTICOLO\n\nDESCRIZIONE\n\nCOLORE\n...
    # followed by actual values on separate lines.
    # Detect this pattern and parse it as a two-row table.
    _parse_hvl_table(text, result)

    # ── Item Code ─────────────────────────────────────────────────────────
    if not result.get("ItemCode"):
        for pat in [
            r"\b(\d{3}\.\d{4}\.\d{3})\b",                          # 560.0755.201 (strict, no trailing)
            r"\b(\d{1,2}\.\d{6,})\b",                              # 3.037280
            r"\b([A-Z0-9]{2,5}[.\-]\d{3,8}[.\-]?\d*)\b",        # generic part codes
            r"(?i)(?:codice\s*articolo|part\s*(?:no|number|code))\s*[:\n]+\s*([^\n\r]{2,50})",
        ]:
            m = re.search(pat, text)
            if m:
                # Hard-truncate to 100 chars — DB column is NVARCHAR(100)
                result["ItemCode"] = m.group(1).strip()[:100]
                break

    # ── Item Description ──────────────────────────────────────────────────
    if not result.get("ItemDescription"):
        for pat in [
            r"(?i)(?:descrizione|description)\s*[:\n]+\s*([^\n\r]{5,200})",
            r"(?i)(?:oggetto|subject)\s*[:\n]+\s*([^\n\r]{5,200})",
        ]:
            m = re.search(pat, text)
            if m:
                result["ItemDescription"] = m.group(1).strip()
                break

    # ── Sender / Customer ─────────────────────────────────────────────────
    from_m = re.search(r"^From:\s*(.+)$", text, re.MULTILINE)
    if from_m:
        raw_from = from_m.group(1).strip()
        # decode RFC2047 encoded words if present (=?Windows-1252?Q?...?=)
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

    if not result.get("Email"):
        em = re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", text)
        if em:
            result["Email"] = em.group(0)

    # Build CustomerIdentityCode from email domain if not set
    if not result.get("CustomerIdentityCode") and result.get("Email"):
        domain = result["Email"].split("@")[-1].split(".")[0].upper()
        result["CustomerIdentityCode"] = f"CUST-{domain}"

    # ── Phone ─────────────────────────────────────────────────────────────
    phone_m = re.search(
        r"(?i)(?:tel|phone|telefono|mob)\s*[:.\s]+([+0-9][\d\s\-().]{6,18})", text
    )
    if phone_m:
        result["TelephoneNumber"] = phone_m.group(1).strip()

    # ── VAT ───────────────────────────────────────────────────────────────
    vat_m = re.search(
        r"(?i)(?:p\.?\s*iva|vat(?:\s*n(?:umber|o)?)?)\s*[:.\s]+([A-Z]{0,3}\d{8,15})", text
    )
    if vat_m:
        result["VatNumber"] = vat_m.group(1).strip()

    # ── RAL color ─────────────────────────────────────────────────────────
    if not result.get("ral_color"):
        ral_m = re.search(r"(?i)\bRAL\s*(\d{4})\b", text)
        if ral_m:
            result["ral_color"] = f"RAL {ral_m.group(1)}"

    # ── Surface area ──────────────────────────────────────────────────────
    if not result.get("total_painting_surface"):
        surf_m = re.search(r"([\d]+[.,][\d]*)\s*m[\xb2\u00b22]", text)
        if surf_m:
            result["total_painting_surface"] = surf_m.group(1).replace(",", ".")

    # ── Batch size ────────────────────────────────────────────────────────
    if not result.get("batch_size"):
        qty_m = re.search(
            r"(?i)(?:batch\s*size|qty|quantity|quantit[àa]|pezzi|pcs)\s*[:\n]+\s*([\d.,]+)", text
        )
        if qty_m:
            result["batch_size"] = qty_m.group(1).strip()

    # ── Subject as order note ─────────────────────────────────────────────
    if not result.get("order_note"):
        subj_m = re.search(r"^Subject:\s*(.+)$", text, re.MULTILINE)
        if subj_m:
            result["order_note"] = subj_m.group(1).strip()

    # Filename fallback for ItemCode
    if not result.get("ItemCode"):
        result["ItemCode"] = _derive_item_code(filename, result)

    return result


def _parse_hvl_table(text: str, result: dict):
    """
    Parse the HVL-specific Italian email table format.

    The body contains a header row with field names separated by blank lines,
    followed by a value row in the same layout:

        CODICE ARTICOLO\n\nDESCRIZIONE\n\nCOLORE\n\nBRILLANTEZZA\n\nFINITURA\n\nBATCH SIZE\n\nPROTEZIONI\n\nNOTE PROTEZIONI\n\n
        560.0755.201\n\nMounting bracket...\n\nRAL 9005\n\nOpaca\n\nLiscia\n\n1.000\n\n/\n\n/\n\n

    We detect the header line and split both rows by double newlines.
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
        "FINITURA":          "finishing_type",   # may override — last wins
        "BATCH SIZE":        "batch_size",
        "PROTEZIONI":        "presence_of_protections",
        "NOTE PROTEZIONI":   "type_of_protections",
    }

    # Normalise line endings
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")

    # Look for header row: "CODICE ARTICOLO" must appear
    if "CODICE ARTICOLO" not in normalized.upper():
        return

    lines = [l.strip() for l in normalized.split("\n")]
    upper_lines = [l.upper() for l in lines]

    # Find the index of "CODICE ARTICOLO"
    try:
        ca_idx = next(i for i, l in enumerate(upper_lines) if l == "CODICE ARTICOLO")
    except StopIteration:
        return

    # Collect header tokens starting from ca_idx; they appear on individual lines
    # separated by blank lines.  Collect until we hit a line that is NOT a known
    # header token AND is not blank.
    headers = []
    values  = []
    i = ca_idx
    header_done = False

    while i < len(lines):
        stripped = lines[i].strip()
        up       = stripped.upper()

        if not header_done:
            if up in (h.upper() for h in HEADER_FIELDS):
                headers.append(stripped.upper())
                i += 1
                continue
            elif stripped == "":
                i += 1
                continue
            else:
                # First non-blank, non-header line → start of values
                header_done = True

        if header_done:
            if stripped == "" and values:
                # blank line after some values — could be between value cells
                i += 1
                continue
            elif stripped != "":
                values.append(stripped)
            i += 1
            if len(values) >= len(headers):
                break

    # Map collected values to result fields
    for col, val in zip(headers, values):
        if val in ("/", "-", "N/A", ""):
            continue
        field = FIELD_MAP.get(col)
        if field and not result.get(field):
            # Normalise RAL color
            if col == "COLORE" and re.match(r"(?i)^ral\s*\d{4}$", val):
                val = re.sub(r"(?i)ral\s*", "RAL ", val)
            result[field] = val

    logger.debug(f"HVL table parse → headers={headers}, values={values}, mapped={result}")


def _derive_item_code(filename: str, result: dict) -> str:
    """Derive a best-effort ItemCode from filename when nothing else works."""
    # Try to pull a numeric/alphanumeric part from the filename
    name = filename.rsplit(".", 1)[0] if "." in filename else filename
    # e.g. "3.037280" → "3.037280", "Rdo 377-25 Franke" → "Rdo_377-25"
    parts = re.findall(r"[\w.\-]+", name)
    if parts:
        code = "_".join(parts[:3])[:50]
    else:
        code = re.sub(r"[^A-Za-z0-9]", "_", name)[:50]

    logger.info(f"Derived ItemCode from filename {filename!r}: {code!r}")
    return code