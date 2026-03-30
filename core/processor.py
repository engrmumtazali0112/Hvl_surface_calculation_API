"""
core/processor.py — File processing: extracts text from PDF/EML/TXT
and runs the HVL extractor pipeline.
"""

import email as _email
import logging
import os
from email import policy as _email_policy
from pathlib import Path

from hvl_extractor import extract_text_from_pdf, extract_regex, finalize_surface_area

log = logging.getLogger("hvl_api")

SUPPORTED_EXT = {".pdf", ".eml", ".txt"}
SKIP_NAMES    = {"readme", "__macosx", ".ds_store"}

# ── Optional AI extractors ────────────────────────────────────────────────────
try:
    from hvl_extractor import extract_claude
    HAS_CLAUDE = True
except ImportError:
    HAS_CLAUDE = False

try:
    from hvl_extractor import extract_gemini
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

try:
    from hvl_extractor import extract_openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


def parse_eml(path: str) -> str:
    """Extract plain-text body + headers from an .eml file."""
    with open(path, "rb") as f:
        raw = f.read()
    msg = _email.message_from_bytes(raw, policy=_email_policy.compat32)
    parts = []
    for part in msg.walk():
        if part.get_content_type() != "text/plain":
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        charset = part.get_content_charset() or "utf-8"
        parts.append(payload.decode(charset, errors="replace"))
    body = "\n".join(parts)
    return (
        f"Subject: {msg.get('Subject', '')}\n"
        f"From: {msg.get('From', '')}\n"
        f"Date: {msg.get('Date', '')}\n\n{body}"
    )


def extract_file(
    path: str,
    claude_key: str = "",
    gemini_key: str = "",
    openai_key: str = "",
) -> dict | None:
    """
    Run the full HVL extraction pipeline on a single file.
    Returns a result dict or None if the file should be skipped.
    """
    ext  = Path(path).suffix.lower()
    name = Path(path).name.lower()

    if any(skip in name for skip in SKIP_NAMES) or ext not in SUPPORTED_EXT:
        return None

    try:
        if ext == ".pdf":
            text, images = extract_text_from_pdf(path), []
        elif ext == ".eml":
            text, images = parse_eml(path), []
        else:
            text, images = open(path, encoding="utf-8", errors="replace").read(), []

        result = _run_ai_extractors(text, images, name, claude_key, gemini_key, openai_key)
        result = finalize_surface_area(result, drawing_text=text)
        result["source_file"] = Path(path).name
        return result

    except Exception as exc:
        log.error("Failed to process %s: %s", name, exc)
        return None


def _run_ai_extractors(
    text: str,
    images: list,
    name: str,
    claude_key: str,
    gemini_key: str,
    openai_key: str,
) -> dict:
    """Try AI extractors in priority order; fall back to regex."""
    result = None

    if claude_key and HAS_CLAUDE:
        try:
            result = extract_claude(text, images, claude_key)
        except Exception as exc:
            log.warning("Claude failed for %s: %s", name, exc)

    if result is None and gemini_key and HAS_GEMINI:
        try:
            result = extract_gemini(text, images, gemini_key)
        except Exception as exc:
            log.warning("Gemini failed for %s: %s", name, exc)

    if result is None and openai_key and HAS_OPENAI:
        try:
            result = extract_openai(text, images, openai_key)
        except Exception as exc:
            log.warning("OpenAI failed for %s: %s", name, exc)

    return result if result is not None else extract_regex(text)
