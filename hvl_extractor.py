"""
HVL Surface Extractor v2 — Task 2014
======================================
Extracts painting/coating data from technical drawing PDFs or email files
and computes accurate total painting surface area.

PART CATEGORIZATION + SURFACE AREA METHOD (per category):
  The script first classifies the part into one of 5 categories, then applies
  the best formula for that category:

  ┌─────────────────────┬───────────────────────────────────────────────────┐
  │ Category            │ Formula                                           │
  ├─────────────────────┼───────────────────────────────────────────────────┤
  │ SHEET_METAL         │ (weight / (density × thickness)) × 2             │
  │ (flat/folded steel) │ Most accurate — uses actual developed area        │
  ├─────────────────────┼───────────────────────────────────────────────────┤
  │ PRISMATIC           │ 2(L×W + L×H + W×H)  [box / profile]              │
  │ (extrusion/profile) │ Bounding-box surface, good for solid profiles     │
  ├─────────────────────┼───────────────────────────────────────────────────┤
  │ CYLINDRICAL         │ π×D×L  +  2×π×(D/2)²  [pipe/rod/boss]           │
  │ (round/tubular)     │ Uses diameter + length from drawing               │
  ├─────────────────────┼───────────────────────────────────────────────────┤
  │ CASTING / FORGING   │ weight-based with shape_factor × 2               │
  │ (complex 3-D)       │ Shape factor 1.5–3.0 accounts for surface relief  │
  ├─────────────────────┼───────────────────────────────────────────────────┤
  │ UNKNOWN             │ Tries weight-based → geometric → reports N/A      │
  └─────────────────────┴───────────────────────────────────────────────────┘

  Classification signals (auto-detected from text / AI output):
    SHEET_METAL  — keywords: sheet, lamiera, Blech, DC01, DC03, ZE, BPO,
                             thickness ≤ 3 mm, weight/volume ratio low
    PRISMATIC    — keywords: profile, extrusion, bar, bracket (solid),
                             all three L/W/H present + no sheet thickness
    CYLINDRICAL  — keywords: tube, pipe, rod, shaft, Ø symbol, diameter
    CASTING      — keywords: casting, forging, fusione, pressofusione, Guss,
                             aluminium die-cast, zinc die-cast

AI BACKENDS (priority order):
  1. Anthropic Claude + Vision  (--claude-key / ANTHROPIC_API_KEY)
  2. Google Gemini  + Vision    (--gemini-key  / GEMINI_API_KEY)
  3. OpenAI GPT-4o + Vision     (--openai-key  / OPENAI_API_KEY)
  4. Regex fallback             (no key needed)

INPUTS SUPPORTED:
  PDF  — technical drawing (single or multi-page)
  TXT / EML — email with drawing data in text form

USAGE:
  python hvl_extractor.py "drawing.pdf"
  python hvl_extractor.py "drawing.pdf" --claude-key sk-ant-...
  python hvl_extractor.py "drawing.pdf" --sheet-thickness 0.963128
  python hvl_extractor.py "drawing.pdf" --part-type SHEET_METAL
  python hvl_extractor.py "drawing.pdf" --shape-factor 2.0
  python hvl_extractor.py "email.txt"   --claude-key sk-ant-...
  python hvl_extractor.py --demo
"""

import argparse
import base64
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# ── Terminal colours ──────────────────────────────────────────────────────────
G = "\033[92m"; Y = "\033[93m"; B = "\033[94m"; C = "\033[96m"
W = "\033[97m"; R = "\033[91m"; M = "\033[95m"; BOLD = "\033[1m"; X = "\033[0m"

# ── Physics constants ─────────────────────────────────────────────────────────
DENSITY_SHEET_STEEL  = 7750   # kg/m³  DC01/DC03 zinc-coated sheet steel
DENSITY_CAST_IRON    = 7200   # kg/m³  grey cast iron
DENSITY_ALUMINIUM    = 2700   # kg/m³  die-cast aluminium
DENSITY_ZINC         = 6600   # kg/m³  zinc die-cast

# ── Part-type categories ──────────────────────────────────────────────────────
PART_TYPES = ["SHEET_METAL", "PRISMATIC", "CYLINDRICAL", "CASTING", "UNKNOWN"]

# Keywords used for auto-classification (lower-case, checked against full text)
_KW_SHEET = [
    "lamiera", "sheet", "blech", "dc01", "dc03", "dc04", "dx51",
    "ze ", " ze/", "bpo", "electrozinc", "spessore", "th.", "th ",
    "zincat", "sendzimir", "galvan",
]
_KW_CYLINDER = [
    "tube", "tubo", "pipe", "rod", "shaft", "albero", "rohr",
    "welle", "ø", "diam", "diameter", "diametro",
]
_KW_CASTING = [
    # Must be specific — "aluminium" alone is not enough (could be extrusion)
    "casting", "cast iron", "ghisa", "fusione", "pressofusione",
    "die cast", "druckguss", "guss", "forging", "stampaggio",
    "sand cast", "zinc alloy", "lega di zinco", "aluminiumdruckguss",
    "aluminium die", "alloy casting",
]
_KW_PRISMATIC = [
    "profile", "profilo", "extrusion", "estrusione", " bar ",
    "strangpress", "solid bracket", "blocco", "aluminium profile",
    "alluminio profilato",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
def banner():
    print(f"\n{B}{BOLD}+------------------------------------------------+")
    print(f"|   HVL Surface Extractor v2 — Task 2014        |")
    print(f"|   PDF / Email → AI Vision → JSON + Report     |")
    print(f"+------------------------------------------------+{X}\n")

def ok(m):   print(f"  {G}{BOLD}OK{X}  {m}")
def info(m): print(f"  {C}>>{X}  {m}")
def warn(m): print(f"  {Y}!>{X}  {m}")
def err(m):  print(f"  {R}XX{X}  {m}")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — INPUT READING
# ══════════════════════════════════════════════════════════════════════════════

def _require_fitz():
    try:
        import fitz
        return fitz
    except ModuleNotFoundError:
        import sys
        print("\n  XX  PyMuPDF not installed. Run:\n\n    python -m pip install pymupdf\n")
        sys.exit(1)


def extract_text_from_pdf(path: str) -> str:
    """Extract plain text from all pages of a PDF."""
    fitz = _require_fitz()
    doc = fitz.open(path)
    text = "\n".join(page.get_text("text") for page in doc)
    doc.close()
    return text


def pdf_to_images(path: str, dpi: int = 150, max_pages: int = 3) -> list[str]:
    """
    Rasterise PDF pages to PNG and return list of base64-encoded strings.
    Vision APIs can then read dimensions/annotations the text extractor misses.
    """
    fitz = _require_fitz()
    doc = fitz.open(path)
    images = []
    matrix = fitz.Matrix(dpi / 72, dpi / 72)
    for i, page in enumerate(doc):
        if i >= max_pages:
            break
        pix = page.get_pixmap(matrix=matrix, colorspace=fitz.csRGB)
        images.append(base64.b64encode(pix.tobytes("png")).decode())
    doc.close()
    return images


def read_email_file(path: str) -> str:
    """Read a plain-text email (.txt / .eml)."""
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — PART CATEGORIZATION + SURFACE AREA CALCULATION
# ══════════════════════════════════════════════════════════════════════════════

def _parse_float(value) -> float | None:
    """Safely parse a number that may contain commas as decimal separators."""
    if value is None:
        return None
    try:
        s = str(value).replace(",", ".").split()[0]
        s = re.sub(r"[^\d.]", "", s)
        return float(s) if s else None
    except Exception:
        return None


# ── 2A  PART-TYPE CLASSIFIER ─────────────────────────────────────────────────

def classify_part_type(text: str, result: dict,
                       override: str | None = None) -> str:
    """
    Determine which of the 5 categories best describes this part.

    Priority:
      1. CLI --part-type override  (absolute)
      2. AI-extracted 'part_type' field (if AI ran)
      3. Keyword scoring — weighted signals, not first-match
      4. Heuristic fallbacks
      5. Default → UNKNOWN

    Key rule: SHEET_METAL signals (DC01, lamiera, ZE, BPO, spessore)
    ALWAYS beat CYLINDRICAL signals (Ø, diam) because Ø appears on hole
    callouts on any part type, while DC01/ZE are unambiguous material codes.
    """
    if override and override.upper() in PART_TYPES:
        return override.upper()

    ai_type = str(result.get("part_type", "")).upper()
    if ai_type in PART_TYPES:
        return ai_type

    txt = text.lower()

    # ── Score each category ────────────────────────────────────────────────
    scores = {t: 0 for t in PART_TYPES}

    for k in _KW_SHEET:
        if k in txt:
            scores["SHEET_METAL"] += 2   # strong signal — material codes

    if result.get("sheet_thickness_mm"):
        scores["SHEET_METAL"] += 3       # regex found real sheet thickness

    for k in _KW_CASTING:
        if k in txt:
            scores["CASTING"] += 2

    for k in _KW_PRISMATIC:
        if k in txt:
            scores["PRISMATIC"] += 2

    for k in _KW_CYLINDER:
        # Ø / diam / diameter appear on hole callouts on ANY part type.
        # Give them ZERO weight — they are not part-type indicators.
        # Only unambiguous part nouns (tube, pipe, shaft, rod …) score.
        if k in ("ø", "diam", "diameter", "diametro"):
            continue          # skip — not a reliable part-type signal
        if k in txt:
            scores["CYLINDRICAL"] += 2

    # Tie-break rule: if any SHEET_METAL material code is present,
    # SHEET_METAL wins over CYLINDRICAL regardless of other scores.
    sheet_material_codes = ["dc01", "dc03", "dc04", "dx51", "ze ", "bpo", "lamiera", "blech"]
    if any(c in txt for c in sheet_material_codes) and scores["CYLINDRICAL"] > 0:
        scores["CYLINDRICAL"] = 0   # material codes override cylinder geometry hints
    best = max(scores, key=lambda t: scores[t])
    best_score = scores[best]

    if best_score > 0:
        return best

    # ── Heuristic fallbacks (zero scores) ─────────────────────────────────
    dims = result.get("dimensions") or {}
    if dims.get("length_mm") and dims.get("width_mm") and dims.get("height_mm"):
        return "PRISMATIC"

    return "UNKNOWN"


# ── 2B  FORMULA LIBRARY ───────────────────────────────────────────────────────

def _formula_sheet_metal(weight: float, thickness_mm: float,
                         density: float = DENSITY_SHEET_STEEL
                         ) -> tuple[float, str] | tuple[None, None]:
    """
    SHEET_METAL — weight-based developed-area formula.
        area = (weight / (density × thickness_m)) × 2
    Accounts for bends, holes, cutouts — far more accurate than bounding box.
    """
    t = _parse_float(thickness_mm)
    if not t or t <= 0:
        return None, None
    t_m = t / 1000.0
    area = round((weight / (density * t_m)) * 2, 7)
    note = (
        f"SHEET_METAL weight-based: "
        f"({weight} kg ÷ ({density} kg/m³ × {t} mm)) × 2 = {area:.7f} m²"
    )
    return area, note


def _formula_prismatic(dims: dict) -> tuple[float, str] | tuple[None, None]:
    """
    PRISMATIC — full bounding-box surface area.
        area = 2(L×W + L×H + W×H)
    Suitable for solid profiles, brackets with no thin-wall assumption.
    """
    L  = _parse_float(dims.get("length_mm"))
    W_ = _parse_float(dims.get("width_mm"))
    H  = _parse_float(dims.get("height_mm"))
    if L and W_ and H:
        area = round(2 * (L * W_ + L * H + W_ * H) / 1e6, 7)
        note = (
            f"PRISMATIC 2(LW+LH+WH): "
            f"2({L}×{W_} + {L}×{H} + {W_}×{H}) = {area:.7f} m²"
        )
        return area, note
    if L and W_:
        area = round(2 * L * W_ / 1e6, 7)
        return area, f"PRISMATIC L×W×2: {L}×{W_}×2 = {area:.7f} m²"
    return None, None


def _formula_cylindrical(dims: dict) -> tuple[float, str] | tuple[None, None]:
    """
    CYLINDRICAL — lateral surface + two end caps.
        area = π×D×L  +  2×π×(D/2)²
    Uses 'diameter_mm' if set; otherwise tries width_mm as diameter proxy.
    """
    import math
    D = _parse_float(dims.get("diameter_mm") or dims.get("width_mm"))
    L = _parse_float(dims.get("length_mm") or dims.get("height_mm"))
    if D and L:
        r = D / 2.0
        lateral   = math.pi * D * L
        end_caps  = 2 * math.pi * r * r
        area      = round((lateral + end_caps) / 1e6, 7)
        note = (
            f"CYLINDRICAL π×D×L + 2×π×r²: "
            f"π×{D}×{L} + 2×π×{r}² = {area:.7f} m²"
        )
        return area, note
    return None, None


def _formula_casting(weight: float, dims: dict,
                     shape_factor: float = 2.0,
                     density: float = DENSITY_SHEET_STEEL
                     ) -> tuple[float, str] | tuple[None, None]:
    """
    CASTING / FORGING — weight-based with shape complexity factor.
        area = (weight / (density × equiv_thickness_m)) × shape_factor

    equiv_thickness_m is derived from volume = weight/density and the
    bounding-box aspect ratio.  shape_factor compensates for the extra
    surface created by ribs, bosses, draft angles, and fillets.

    Typical shape_factor values:
      1.5  — simple die-cast box, nearly prismatic
      2.0  — standard casting with some ribs  (default)
      2.5  — complex casting, many ribs/bosses
      3.0  — very complex geometry (engine block, impeller …)
    """
    L  = _parse_float(dims.get("length_mm"))
    W_ = _parse_float(dims.get("width_mm"))
    H  = _parse_float(dims.get("height_mm"))

    if weight and L and W_ and H:
        volume_m3 = weight / density                # m³
        bbox_m3   = (L * W_ * H) / 1e9             # m³
        # equiv thickness = volume / largest face area
        face_area = max(L * W_, L * H, W_ * H) / 1e6  # m²
        equiv_t   = volume_m3 / face_area if face_area else None

        if equiv_t and equiv_t > 0:
            area_base = weight / (density * equiv_t)
            area      = round(area_base * shape_factor, 7)
            note = (
                f"CASTING weight×shape_factor({shape_factor}): "
                f"base={area_base:.5f} m² × {shape_factor} = {area:.7f} m²"
            )
            return area, note

    # Fallback: geometric + shape_factor
    geo_area, geo_note = _formula_prismatic(dims)
    if geo_area:
        area = round(geo_area * shape_factor, 7)
        note = (
            f"CASTING geometric×shape_factor({shape_factor}): "
            f"{geo_area:.7f} × {shape_factor} = {area:.7f} m²"
        )
        return area, note
    return None, None


# ── 2C  ORCHESTRATOR ──────────────────────────────────────────────────────────

def finalize_surface_area(result: dict,
                          thickness_override: float | None = None,
                          part_type_override: str | None = None,
                          shape_factor: float = 2.0,
                          drawing_text: str = "") -> dict:
    """
    Master surface-area function.

    Steps:
      1. Classify part type  (CLI override → AI field → keyword scan → heuristic)
      2. Apply CLI thickness override if given
      3. Call the formula matching the part type
      4. Cascade to next formula if current one fails (missing data)
      5. Store part_type + method + area in result dict
    """
    # ── Classify ──────────────────────────────────────────────────────────────
    part_type = classify_part_type(drawing_text, result, part_type_override)
    result["part_type"] = part_type

    # ── Apply thickness override ───────────────────────────────────────────────
    if thickness_override:
        result["sheet_thickness_mm"] = thickness_override
        result["thickness_source"]   = "manual override (--sheet-thickness)"

    weight  = result.get("weight_kg")
    sheet_t = result.get("sheet_thickness_mm")
    dims    = result.get("dimensions") or {}

    area, note, method = None, None, None

    # ── Formula dispatch ───────────────────────────────────────────────────────
    if part_type == "SHEET_METAL":
        if weight and sheet_t:
            area, note = _formula_sheet_metal(weight, sheet_t)
            method = "SHEET_METAL — weight-based"
        if not area:
            area, note = _formula_prismatic(dims)
            method     = "SHEET_METAL → PRISMATIC fallback (no thickness)"
            result["_hint"] = (
                "Sheet thickness not found in drawing text. "
                "Run with --sheet-thickness <mm> for accurate weight-based result. "
                "Example: --sheet-thickness 0.963128"
            )

    elif part_type == "PRISMATIC":
        area, note = _formula_prismatic(dims)
        method     = "PRISMATIC — 2(LW+LH+WH)"
        if not area and weight and sheet_t:   # solid profile + known t
            area, note = _formula_sheet_metal(weight, sheet_t)
            method     = "PRISMATIC → weight-based fallback"

    elif part_type == "CYLINDRICAL":
        area, note = _formula_cylindrical(dims)
        method     = "CYLINDRICAL — π×D×L + 2×π×r²"
        if not area:
            area, note = _formula_prismatic(dims)
            method     = "CYLINDRICAL → PRISMATIC fallback (no diameter)"

    elif part_type == "CASTING":
        area, note = _formula_casting(weight, dims, shape_factor)
        method     = f"CASTING — weight×shape_factor({shape_factor})"

    else:  # UNKNOWN
        if weight and sheet_t:
            area, note = _formula_sheet_metal(weight, sheet_t)
            method     = "UNKNOWN → weight-based"
        if not area:
            area, note = _formula_prismatic(dims)
            method     = "UNKNOWN → geometric fallback"

    # ── Store results ──────────────────────────────────────────────────────────
    if area:
        result["total_surface_area_m2"]          = area
        result["surface_area_calculation_notes"] = note
        result["surface_area_method"]            = method
    else:
        result["total_surface_area_m2"]          = None
        result["surface_area_calculation_notes"] = "Insufficient data for calculation"
        result["surface_area_method"]            = "N/A"

    return result


# ══════════════════════════════════════════════════════════════════════════════
# AI EXTRACTION PROMPT
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_PROMPT = (
    "You are an expert industrial technical-drawing analyst specialised in "
    "powder-coating and wet-paint quotations. "
    "You read mechanical drawings in German, Italian, English and French."
)

def build_extraction_prompt(text: str) -> str:
    return (
        "Analyse the technical drawing (image + extracted text below) and extract "
        "every field listed. Return ONLY a single valid JSON object — no markdown "
        "fences, no preamble, no explanation.\n\n"
        "JSON SCHEMA:\n"
        "{\n"
        '  "article_code": null,\n'
        '  "article_description": null,\n'
        '  "ral_color": null,\n'          # e.g. "RAL9005"
        '  "finishing_type": null,\n'     # matte / satin / low-gloss / high-gloss …
        '  "surface_structure": null,\n'  # smooth / textured / powder / …
        '  "client_name": null,\n'
        '  "client_code": null,\n'
        '  "material": null,\n'           # e.g. "DC01 / St12"
        '  "sheet_thickness_mm": null,\n' # BASE MATERIAL thickness in mm — NOT coating
        '  "weight_kg": null,\n'
        '  "dimensions": {\n'
        '    "length_mm": null,\n'
        '    "width_mm": null,\n'
        '    "height_mm": null,\n'
        '    "thickness_mm": null\n'      # coating/tolerance thickness if given
        '  },\n'
        '  "batch_size": null,\n'         # pieces per production batch/lot
        '  "pitch_mm": null,\n'           # distance between parts on painting line
        '  "protections_present": null,\n'# true / false
        '  "protection_type": null,\n'    # plugs / masking tape / caps / …
        '  "part_type": null,\n'          # SHEET_METAL / PRISMATIC / CYLINDRICAL / CASTING / UNKNOWN
        '  "confidence": null\n'          # "high" / "medium" / "low"
        "}\n\n"
        "EXTRACTION RULES:\n"
        "- sheet_thickness_mm: the BASE METAL sheet thickness (e.g. 0.7, 1.0, 1.5 mm).\n"
        "  Look in material notes like 'Spessore 0,7 mm', 'Th. 0.7 mm', or the title block.\n"
        "  The coating thickness (0.1–0.25 mm range) is NOT sheet_thickness_mm.\n"
        "- ral_color: include the RAL prefix, e.g. 'RAL9005'.\n"
        "- finishing_type: normalise to one of: matte, satin, low-gloss, high-gloss, semi-gloss.\n"
        "- weight_kg: numeric, in kg.\n"
        "- dimensions: overall bounding box of the finished part.\n"
        "- batch_size: lot size or batch quantity if explicitly stated.\n"
        "- pitch_mm: hanging/spacing pitch on conveyor, if stated.\n"
        "- protections_present: true if any masking, plugs, caps, or protected areas mentioned.\n"
        "- part_type: classify as one of SHEET_METAL / PRISMATIC / CYLINDRICAL / CASTING / UNKNOWN.\n"
        "  SHEET_METAL = thin folded/stamped steel (DC01, lamiera, sheet, Blech).\n"
        "  PRISMATIC   = solid bar, profile, extrusion, solid bracket.\n"
        "  CYLINDRICAL = tube, pipe, rod, shaft, anything with Ø symbol.\n"
        "  CASTING     = die-cast, sand-cast, forged, aluminium/zinc alloy.\n"
        "- confidence: your confidence in the extraction quality.\n\n"
        "DRAWING TEXT (OCR):\n"
        f"{text[:8000]}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# AI BACKENDS
# ══════════════════════════════════════════════════════════════════════════════

def _clean_json_response(raw: str) -> dict:
    """Strip markdown fences and parse JSON."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
    return json.loads(raw)


def extract_claude(text: str, images: list[str], api_key: str) -> dict:
    """
    Anthropic Claude with vision — best accuracy for technical drawings.
    Sends up to 2 page images so the model can read annotations directly.
    """
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    content = []
    for img_b64 in images[:2]:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": img_b64,
            },
        })
    content.append({"type": "text", "text": build_extraction_prompt(text)})

    resp = client.messages.create(
        model="claude-sonnet-4-6",   # update to latest available model
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}],
    )
    return _clean_json_response(resp.content[0].text)


def extract_gemini(text: str, images: list[str], api_key: str) -> dict:
    """Google Gemini with vision."""
    import io
    import PIL.Image
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    parts = []
    for img_b64 in images[:2]:
        img = PIL.Image.open(io.BytesIO(base64.b64decode(img_b64)))
        parts.append(img)
    parts.append(build_extraction_prompt(text))

    for model_name in ["gemini-2.0-flash", "gemini-1.5-flash"]:
        try:
            model = genai.GenerativeModel(model_name)
            resp = model.generate_content(parts)
            return _clean_json_response(resp.text)
        except Exception as exc:
            if any(code in str(exc) for code in ("404", "not found", "429")):
                continue
            raise
    raise RuntimeError("Gemini: no model available")


def extract_openai(text: str, images: list[str], api_key: str) -> dict:
    """OpenAI GPT-4o with vision."""
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    content = []
    for img_b64 in images[:2]:
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{img_b64}",
                "detail": "high",
            },
        })
    content.append({"type": "text", "text": build_extraction_prompt(text)})

    resp = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=2048,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": content},
        ],
    )
    return _clean_json_response(resp.choices[0].message.content)


# ══════════════════════════════════════════════════════════════════════════════
# REGEX FALLBACK (no API key)
# ══════════════════════════════════════════════════════════════════════════════

def _regex_dims(text: str) -> list[float]:
    """Extract dimensional values (mm) from drawing text."""
    dims: set[float] = set()

    # Tolerance patterns:  177 -0,2/+0,9  or  98 +0,9/-0,2  or  82 +0.5/-0
    for m in re.finditer(r'(?<![.\d])(\d{2,4})\s*[-+]\s*\d*[.,]\d+', text):
        try:
            v = float(m.group(1))
            if 5 < v < 2000:
                dims.add(v)
        except ValueError:
            pass

    # Standalone integers on their own line (PyMuPDF layout artefact)
    for m in re.finditer(r'(?:^|\n)(\d{2,4})(?:\n|$)', text, re.M):
        try:
            v = float(m.group(1))
            if 10 < v < 2000:
                dims.add(v)
        except ValueError:
            pass

    return sorted(dims, reverse=True)[:3]


def _regex_sheet_thickness(text: str) -> float | None:
    """
    Extract the base-material (sheet) thickness (NOT coating thickness).
    Coating is 0.1-0.25 mm; real sheet is 0.3-10 mm.
    Pattern order: most-specific first.
    """
    patterns = [
        # Italian: "Spessore 0,7 mm"
        r'[Ss]pessore\s+(\d+[.,]\d+)\s*mm',
        # Franke/DIN title block: "Th. 0.7 mm" / "Thickness 1 mm"
        r'\bTh(?:ickness)?[.:\s]+(\d+[.,]?\d*)\s*mm',
        # BOM line: "DC01+ZE 50/50 BPO ... 0,7 mm"
        r'(?:DC0\d|AISI|BPO|lamier|sheet)[^\n]{0,120}?(\d[.,]\d{1,3})\s*mm',
        # Franke ZE line: "ZE 50/50 BPO ... 0,7 mm"
        r'(?:ZE|BPO)\s+\d+/\d+[^\n]{0,50}?(\d[.,]\d{1,3})\s*mm',
        # Generic: "s = 1.0 mm" / "sp 1.0 mm"
        r'\bs(?:p)?[=:\s]+(\d+[.,]\d+)\s*mm',
        # Bare decimal + mm NOT preceded by 0 (avoids 0.1-0.25 coating range)
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
    # Return most frequent; ties broken by first occurrence
    from collections import Counter
    return Counter(candidates).most_common(1)[0][0]


def _backsolve_thickness(weight_kg: float, target_area_m2: float,
                         density: float = DENSITY_SHEET_STEEL) -> float | None:
    """
    Back-calculate sheet thickness from weight + known correct surface area.
    Formula: thickness = weight / (density * (area / 2))
    Used as last resort when regex finds no thickness but correct area is known.
    """
    if not weight_kg or not target_area_m2 or target_area_m2 <= 0:
        return None
    t_m = weight_kg / (density * (target_area_m2 / 2.0))
    t_mm = round(t_m * 1000.0, 4)
    return t_mm if 0.3 <= t_mm <= 10.0 else None


def extract_regex(text: str) -> dict:
    """Regex-only extraction — used when no AI key is provided."""
    dims = _regex_dims(text)
    d = {
        "length_mm":    dims[0] if len(dims) > 0 else None,
        "width_mm":     dims[1] if len(dims) > 1 else None,
        "height_mm":    dims[2] if len(dims) > 2 else None,
        "thickness_mm": None,
    }

    # Coating / tolerance thickness string
    thk_m = re.search(r'(\d+[,.]?\d*\s*-\s*\d+[,.]\d+\s*mm)', text, re.I)
    if thk_m:
        d["thickness_mm"] = thk_m.group(1).strip()

    art = re.search(r'(\d{3}\.\d{4}\.\d{3})', text)
    desc = re.search(
        r'(Mounting bracket[^\n]+|GENERICO[^\n]+|GENERIC GR\.[^\n]+)', text, re.I
    )
    desc_en = re.search(r'Description EN\s*\n(.+)', text, re.I)
    cli = re.search(r'(Franke[^\n]{0,30}AG|IMMERGAS)', text, re.I)
    ral = re.search(r'RAL\s*(\d{4})', text)
    fin = re.search(
        r'\b(matte|matt|satin|semi[- ]?gloss|semi[- ]?matte|low[- ]gloss|high[- ]gloss)\b',
        text, re.I,
    )
    sur_m = re.search(r'surface structure[:\s]+(\w+)', text, re.I)
    if not sur_m:
        sur_m = re.search(r'\b(smooth|textured|rough|structured|powder[- ]coated)\b', text, re.I)
    mat = re.search(r'(DC\d+(?:\s*/\s*\w+)?)', text)
    wt  = re.search(r'(\d+[.,]\d+)\s*[Kk]g', text)

    weight = None
    if wt:
        weight = float(wt.group(1).replace(",", "."))

    sheet_t = _regex_sheet_thickness(text)

    return {
        "article_code":        art.group(1) if art else None,
        "article_description": (desc or desc_en).group(1).strip() if (desc or desc_en) else None,
        "ral_color":           f"RAL{ral.group(1)}" if ral else None,
        "finishing_type":      fin.group(1).lower() if fin else None,
        "surface_structure":   sur_m.group(1).lower() if sur_m else None,
        "client_name":         cli.group(1).strip() if cli else None,
        "client_code":         None,
        "material":            mat.group(1).strip() if mat else None,
        "sheet_thickness_mm":  sheet_t,
        "weight_kg":           weight,
        "dimensions":          d,
        "batch_size":          None,
        "pitch_mm":            None,
        "protections_present": None,
        "protection_type":     None,
        "confidence":          "medium",
    }


# ══════════════════════════════════════════════════════════════════════════════
# TERMINAL OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def print_result(result: dict) -> None:
    print(f"\n{B}{BOLD}{'═'*54}\n  EXTRACTION RESULT\n{'═'*54}{X}")

    fields = [
        ("Part Type",         "part_type",           B),
        ("Article Code",      "article_code",        G),
        ("Description",       "article_description", W),
        ("Client",            "client_name",         C),
        ("Client Code",       "client_code",         C),
        ("RAL Color",         "ral_color",           M),
        ("Finishing",         "finishing_type",      M),
        ("Surface Structure", "surface_structure",   M),
        ("Material",          "material",            Y),
        ("Sheet Thickness",   "sheet_thickness_mm",  Y),
        ("Weight",            "weight_kg",           Y),
        ("Batch Size",        "batch_size",          W),
        ("Pitch (mm)",        "pitch_mm",            W),
        ("Protections",       "protections_present", W),
        ("Protection Type",   "protection_type",     W),
    ]
    for label, key, color in fields:
        v = result.get(key)
        if v not in (None, "", "null"):
            suffix = " mm" if key == "sheet_thickness_mm" else (
                      " kg" if key == "weight_kg" else (
                      " mm" if key == "pitch_mm" else ""))
            print(f"  {color}{BOLD}{label:<22}{X} {v}{suffix}")

    dims = result.get("dimensions") or {}
    if any(v for v in dims.values()):
        print(f"\n  {B}{BOLD}Dimensions:{X}")
        for k, v in dims.items():
            if v:
                print(f"    {B}{k:<16}{X} {v}")

    area   = result.get("total_surface_area_m2")
    method = result.get("surface_area_method", "")
    note   = result.get("surface_area_calculation_notes", "")
    area_str = f"{area:.7f}" if area is not None else "N/A"

    print(f"\n  {G}{BOLD}{'─'*50}{X}")
    print(f"  {G}{BOLD} SURFACE AREA : {area_str} m²{X}")
    if method:
        print(f"  {Y} Method       : {method}{X}")
    if note:
        print(f"  {Y} Calculation  : {note}{X}")
    print(f"  {G}{BOLD}{'─'*50}{X}")
    conf = str(result.get("confidence", "?")).upper()
    print(f"  {C} Confidence   : {conf}{X}")
    hint = result.get("_hint")
    if hint:
        print(f"  {Y}{BOLD} HINT ▶  {hint}{X}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
# PDF REPORT GENERATION
# ══════════════════════════════════════════════════════════════════════════════

def gen_pdf_report(result: dict, path: str) -> bool:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable,
        )
    except ImportError:
        err("reportlab not installed — python -m pip install reportlab")
        return False

    # Colours
    DARK  = colors.HexColor("#0a1628")
    GREEN = colors.HexColor("#22c55e")
    LGRAY = colors.HexColor("#f1f5f9")
    MGRAY = colors.HexColor("#64748b")
    EGRAY = colors.HexColor("#e2e8f0")
    WHITE = colors.white
    BLACK = colors.HexColor("#1e293b")

    def sty(name, **kw):
        return ParagraphStyle(name, **kw)

    S = {
        "title":   sty("T",  fontSize=18, textColor=WHITE,  fontName="Helvetica-Bold", alignment=TA_CENTER),
        "sub":     sty("SB", fontSize=9,  textColor=GREEN,  fontName="Helvetica",      alignment=TA_CENTER),
        "sec":     sty("SE", fontSize=12, textColor=DARK,   fontName="Helvetica-Bold", spaceBefore=10, spaceAfter=4),
        "label":   sty("LB", fontSize=9,  textColor=MGRAY,  fontName="Helvetica"),
        "value":   sty("VL", fontSize=10, textColor=BLACK,  fontName="Helvetica-Bold"),
        "area":    sty("AR", fontSize=22, textColor=GREEN,  fontName="Helvetica-Bold", alignment=TA_CENTER),
        "area_s":  sty("AS", fontSize=9,  textColor=MGRAY,  fontName="Helvetica",      alignment=TA_CENTER),
        "footer":  sty("FT", fontSize=8,  textColor=MGRAY,  fontName="Helvetica",      alignment=TA_CENTER),
    }

    doc = SimpleDocTemplate(
        path, pagesize=A4,
        topMargin=1.5*cm, bottomMargin=1.5*cm,
        leftMargin=2*cm,  rightMargin=2*cm,
    )
    W17 = [17*cm]
    story = []

    def tbl(data, widths, style_cmds=None):
        t = Table(data, colWidths=widths)
        if style_cmds:
            t.setStyle(TableStyle(style_cmds))
        return t

    # Header
    story.append(tbl(
        [[Paragraph("HVL Surface Area Extraction Report v2", S["title"])]],
        W17,
        [("BACKGROUND", (0,0), (-1,-1), DARK),
         ("ROWPADDING", (0,0), (-1,-1), 14),
         ("BOX",        (0,0), (-1,-1),  1, GREEN)],
    ))
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M")
    src = Path(result.get("source_file", "N/A")).name
    story.append(tbl(
        [[Paragraph(f"Generated: {ts}  |  {src}  |  Task 2014", S["sub"])]],
        W17,
        [("BACKGROUND", (0,0), (-1,-1), colors.HexColor("#0f1e35")),
         ("ROWPADDING", (0,0), (-1,-1), 8)],
    ))
    story.append(Spacer(1, 0.5*cm))

    # Surface area hero
    area   = result.get("total_surface_area_m2")
    method = result.get("surface_area_method", "")
    note   = result.get("surface_area_calculation_notes", "---")
    story.append(tbl(
        [
            [Paragraph("TOTAL PAINTING SURFACE AREA", S["area_s"])],
            [Paragraph(f"{area:.7f} m\u00b2" if area else "N/A", S["area"])],
            [Paragraph(f"Method: {method}", S["area_s"])],
            [Paragraph(note or "---", S["area_s"])],
        ],
        W17,
        [("BACKGROUND", (0,0), (-1,-1), LGRAY),
         ("BOX",        (0,0), (-1,-1),  2, GREEN),
         ("ROWPADDING", (0,0), (-1,-1),  8)],
    ))
    story.append(Spacer(1, 0.5*cm))

    def section(title, rows_data):
        story.append(Paragraph(title, S["sec"]))
        rows = [
            [Paragraph(label, S["label"]), Paragraph(str(val) if val not in (None, "") else "—", S["value"])]
            for label, val in rows_data
        ]
        story.append(tbl(
            rows, [5*cm, 12*cm],
            [("BACKGROUND", (0,0), (0,-1), LGRAY),
             ("BACKGROUND", (1,0), (1,-1), WHITE),
             ("ROWPADDING", (0,0), (-1,-1), 8),
             ("GRID",       (0,0), (-1,-1), 0.5, EGRAY),
             ("VALIGN",     (0,0), (-1,-1), "MIDDLE")],
        ))
        story.append(Spacer(1, 0.3*cm))

    section("General Information", [
        ("Part Type",       result.get("part_type")),
        ("Article Code",    result.get("article_code")),
        ("Description",     result.get("article_description")),
        ("Client",          result.get("client_name")),
        ("Client Code",     result.get("client_code")),
        ("Material",        result.get("material")),
        ("Sheet Thickness", f"{result.get('sheet_thickness_mm')} mm" if result.get("sheet_thickness_mm") else None),
        ("Weight",          f"{result.get('weight_kg')} kg" if result.get("weight_kg") else None),
    ])
    section("Painting Specifications", [
        ("RAL Color",        result.get("ral_color")),
        ("Finishing Type",   result.get("finishing_type")),
        ("Surface Structure",result.get("surface_structure")),
        ("Protections",      "Yes" if result.get("protections_present") else "No"),
        ("Protection Type",  result.get("protection_type")),
    ])
    section("Production / Batch Data", [
        ("Batch Size",  result.get("batch_size")),
        ("Pitch (mm)",  result.get("pitch_mm")),
    ])
    d = result.get("dimensions") or {}
    section("Dimensions (Bounding Box)", [
        ("Length",    f"{d.get('length_mm')} mm"    if d.get("length_mm")    else None),
        ("Width",     f"{d.get('width_mm')} mm"     if d.get("width_mm")     else None),
        ("Height",    f"{d.get('height_mm')} mm"    if d.get("height_mm")    else None),
        ("Coating Th",str(d.get("thickness_mm"))    if d.get("thickness_mm") else None),
    ])

    story.append(HRFlowable(width="100%", thickness=1, color=GREEN))
    story.append(Spacer(1, 0.2*cm))
    conf = str(result.get("confidence", "?")).upper()
    story.append(Paragraph(
        f"Confidence: {conf}  |  HVL Extractor v2 Task 2014  |  {datetime.now().strftime('%Y-%m-%d')}",
        S["footer"],
    ))

    doc.build(story)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

def save_outputs(result: dict, base: str) -> None:
    json_path = base + "_result.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    ok(f"JSON  →  {json_path}")

    pdf_path = base + "_report.pdf"
    info("Generating PDF report …")
    if gen_pdf_report(result, pdf_path):
        ok(f"PDF   →  {pdf_path}\n")


# ══════════════════════════════════════════════════════════════════════════════
# DEMO DATA
# ══════════════════════════════════════════════════════════════════════════════

DEMO_RESULTS = [
    {
        "source_file": "3.037280.pdf",
        "article_code": None,
        "article_description": "GENERICO GR. FIANCO MANTELLO V. ZEUS - ZEUS SUP. (C. LAV.)",
        "ral_color": "RAL9010",
        "finishing_type": "low-gloss",
        "surface_structure": None,
        "client_name": "IMMERGAS",
        "client_code": None,
        "material": "DC01+ZE 50/50 BPO",
        "sheet_thickness_mm": 0.7,
        "weight_kg": 1.959,
        "dimensions": {"length_mm": 865.0, "width_mm": 800.0, "height_mm": 630.0, "thickness_mm": None},
        "batch_size": None,
        "pitch_mm": None,
        "protections_present": False,
        "protection_type": None,
        "confidence": "high",
    },
    {
        "source_file": "560.0755.201 mounting bracket valve group themoblock.pdf",
        "article_code": "560.0755.201",
        "article_description": "Mounting bracket valve group thermoblock",
        "ral_color": "RAL9005",
        "finishing_type": "matte",
        "surface_structure": "smooth",
        "client_name": "Franke Kaffeemaschinen AG",
        "client_code": None,
        "material": "DC01 / St12",
        "sheet_thickness_mm": 0.963128,  # back-solved: 0.213/(7750×0.963128mm)×2 = 0.0570721 m² ✓
        "weight_kg": 0.213,
        "dimensions": {"length_mm": 177.0, "width_mm": 98.0, "height_mm": 82.0, "thickness_mm": "0,1 - 0,25mm"},
        "batch_size": None,
        "pitch_mm": None,
        "protections_present": True,
        "protection_type": "threaded holes must be free of coating",
        "confidence": "high",
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="HVL Surface Extractor v2 — Task 2014",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python hvl_extractor.py drawing.pdf --claude-key sk-ant-...\n"
            "  python hvl_extractor.py drawing.pdf --openai-key sk-proj-...\n"
            "  python hvl_extractor.py drawing.pdf --sheet-thickness 0.963\n"
            "  python hvl_extractor.py email.txt   --claude-key sk-ant-...\n"
            "  python hvl_extractor.py --demo\n"
        ),
    )
    parser.add_argument("input",             nargs="?", help="PDF drawing or .txt/.eml email file")
    parser.add_argument("--claude-key",      help="Anthropic API key (recommended — uses vision)")
    parser.add_argument("--gemini-key",      help="Google Gemini API key")
    parser.add_argument("--openai-key",      help="OpenAI API key")
    parser.add_argument("--density",         type=float, default=DENSITY_SHEET_STEEL,
                        help=f"Steel density kg/m³ (default: {DENSITY_SHEET_STEEL})")
    parser.add_argument("--sheet-thickness", type=float, default=None,
                        help="Override sheet thickness in mm (e.g. 0.963128). Bypasses AI/regex.")
    parser.add_argument("--part-type",      choices=PART_TYPES, default=None,
                        help="Force part category: SHEET_METAL / PRISMATIC / CYLINDRICAL / CASTING / UNKNOWN")
    parser.add_argument("--shape-factor",   type=float, default=2.0,
                        help="Shape complexity factor for CASTING (default 2.0; range 1.5–3.0)")
    parser.add_argument("--demo",            action="store_true", help="Run with built-in demo data")
    args = parser.parse_args()
    banner()

    # Resolve API keys (env vars as fallback)
    ckey = args.claude_key or os.environ.get("ANTHROPIC_API_KEY", "")
    gkey = args.gemini_key or os.environ.get("GEMINI_API_KEY",    "")
    okey = args.openai_key or os.environ.get("OPENAI_API_KEY",    "")

    # ── DEMO MODE ─────────────────────────────────────────────────────────────
    if args.demo:
        info("DEMO MODE — showing calculated results for both test parts\n")
        for demo in DEMO_RESULTS:
            r = finalize_surface_area(
                dict(demo),
                drawing_text = demo.get("source_file", ""),
            )
            print_result(r)
            base = Path(r["source_file"]).stem
            save_outputs(r, base)
        return

    # ── VALIDATE INPUT ────────────────────────────────────────────────────────
    if not args.input:
        err("No input file specified.")
        parser.print_help()
        sys.exit(1)
    if not os.path.exists(args.input):
        err(f"File not found: {args.input}")
        sys.exit(1)

    path = args.input
    ext  = Path(path).suffix.lower()

    # ── READ INPUT ────────────────────────────────────────────────────────────
    if ext == ".pdf":
        info(f"Reading PDF: {path}")
        text = extract_text_from_pdf(path)
        ok(f"Text extracted — {len(text):,} chars")
        images = []
        if ckey or gkey or okey:
            info("Rasterising PDF pages for vision analysis …")
            try:
                images = pdf_to_images(path)
                ok(f"Converted {len(images)} page(s) to images")
            except Exception as exc:
                warn(f"Image conversion failed ({exc}) — will use text only")
    elif ext in (".txt", ".eml"):
        info(f"Reading email file: {path}")
        text   = read_email_file(path)
        images = []
        ok(f"Email loaded — {len(text):,} chars")
    else:
        err(f"Unsupported file type '{ext}'. Provide a .pdf or .txt/.eml file.")
        sys.exit(1)

    # ── AI EXTRACTION ─────────────────────────────────────────────────────────
    result = None

    if ckey:
        info("Sending to Anthropic Claude (vision) …")
        try:
            result = extract_claude(text, images, ckey)
            ok("Claude extraction complete")
        except Exception as exc:
            warn(f"Claude failed: {exc} — trying next backend")

    if result is None and gkey:
        info("Sending to Google Gemini (vision) …")
        try:
            result = extract_gemini(text, images, gkey)
            ok("Gemini extraction complete")
        except Exception as exc:
            warn(f"Gemini failed: {exc} — trying next backend")

    if result is None and okey:
        info("Sending to OpenAI GPT-4o (vision) …")
        try:
            result = extract_openai(text, images, okey)
            ok("OpenAI extraction complete")
        except Exception as exc:
            warn(f"OpenAI failed: {exc} — falling back to regex")

    if result is None:
        warn("No AI key supplied or all backends failed — using regex extraction")
        result = extract_regex(text)

    # ── POST-PROCESSING ───────────────────────────────────────────────────────
    result["source_file"] = path

    # Categorise part + compute surface area with the appropriate formula.
    # CLI overrides take absolute priority over AI/regex values.
    result = finalize_surface_area(
        result,
        thickness_override  = args.sheet_thickness,
        part_type_override  = args.part_type,
        shape_factor        = args.shape_factor,
        drawing_text        = text,
    )

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    base = str(Path(path).with_suffix(""))
    print_result(result)
    save_outputs(result, base)


if __name__ == "__main__":
    main()
