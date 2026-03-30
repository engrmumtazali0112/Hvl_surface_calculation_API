
# HVL Surface API
**Task 2014 & 2047 — Automatic extraction and database insertion of surface treatment orders**

Reads a ZIP file containing PDF drawings, `.eml`, and `.msg` emails, extracts all data needed for a painting quotation — article code, RAL colour, finishing, batch size, surface area — and automatically inserts the results into SQL Server.

---

## ✨ What It Does

- Accepts a **ZIP file** containing PDFs, `.eml`, and `.msg` files
- Extracts part info: ItemCode, RAL color, finishing, weight, surface area, customer
- Inserts / updates records into SQL Server: **Items → Customers → Orders → OrderRows → OrderValues**
- Generates sequential order numbers: `OFF-HVL-2026-000001`
- AI extraction (Claude → Gemini → OpenAI) with **regex fallback** for reliability

---

## ⚡ Quick Start

```powershell
pip install -r requirements.txt
# Add your DB credentials to .env
python -m uvicorn main:app --reload --port 8000
```

API docs available at: `http://127.0.0.1:8000/docs`

---

## 📁 Project Structure

```
hvl_surface/
│
├── main.py              # App entry point (FastAPI)
├── config.py            # Environment & DB config
├── extractor.py         # Extraction engine (AI + regex fallback)
├── hvl_api.py           # API route: POST /api/v1/process
├── hvl_batch.py         # Optional local batch runner (no DB)
│
├── api/                 # API layer
├── core/                # Business logic
├── db/                  # DB connection & operations
│
├── 01_inputs/           # Input files (local batch mode)
│     drawing.pdf
│     request.eml
│
├── 02_outputs/          # Results (local batch mode)
│     ├── json/          →  one _result.json per file
│     ├── reports/       →  one _report.pdf  per file
│     └── excel/         →  HVL_Summary.xlsx (all parts)
│
├── 03_archive/          # Move processed inputs here when done
├── requirements.txt     # Python dependencies
└── .env                 # DB credentials (do NOT commit)
```

---

## 🌐 API Endpoint

**POST** `/api/v1/process`

| Field | Type | Description |
|---|---|---|
| `file` | ZIP | Contains PDFs / `.eml` / `.msg` files |
| `company_id` | int | ID from `Login.dbo.Companies` |
| `environment` | string | `prod` / `dev` / `uat` / `test` |
| `modifier_user` | string | User tag for DB audit fields |
| `claude_key` | string | *(optional)* Anthropic API key |
| `gemini_key` | string | *(optional)* Google Gemini key |
| `openai_key` | string | *(optional)* OpenAI key |

**Example Response:**
```json
{
  "company_db": "Demo_Etwin-dev",
  "environment": "dev",
  "processed": [
    {
      "file": "Rdo 377-25 Franke.eml",
      "status": "ok",
      "NOrder": "OFF-HVL-2026-000015",
      "IdItem": 825698,
      "IdCustomer": 2993,
      "IdOrder": 485572,
      "IdOrderRow": 426686
    }
  ]
}
```

---

## 📥 Supported Input Files

| File type | Example | Notes |
|---|---|---|
| PDF drawing | `560.0755.201.pdf` | Single or multi-page |
| Email file | `Rdo_377-25_Franke.eml` | Saved from Outlook |
| Outlook msg | `Richiesta IMMERGAS.msg` | Exported from Outlook |

---

## 🔍 What Gets Extracted

| Field | Source |
|---|---|
| Article code | Drawing title block / email |
| Article description | Drawing / email |
| RAL colour | Drawing finish spec / email |
| Finishing type | `matte` / `semi-gloss` / `low-gloss` … |
| Batch size | Email table |
| Pitch (mm) | Email table (conveyor spacing) |
| Protections present | Drawing notes / email |
| Client name | Drawing title block / email From: |
| **Total painting surface area (m²)** | Calculated — weight + sheet thickness |

### Surface Area Formula (SHEET_METAL)
```
area = (weight_kg / (density_kg_m³ × thickness_m)) × 2
```
Accounts for bends, holes, and cutouts using actual part weight and sheet thickness.

---

## 🗄️ Database Tables Written

| Table | Key | Action |
|---|---|---|
| `Items` | `ItemCode` | Insert or Update |
| `Customers` | `CustomerIdentityCode` | Insert or Update |
| `Orders` | `NOrder` (sequential) | Insert |
| `OrderRows` | `IdOrderParent` + `IdItem` + `IdCustomer` | Insert |
| `OrderValues` | RAL (27), Finishing (28), Batch (29), Surface area (33) | Insert |

---

## ⚙️ Environment Variables (`.env`)

```env
DB_SERVER=host8728.shserver.it
DB_PORT=1438
DB_USER=sa
DB_PASSWORD=your_password
```

---

## 🔗 Environment Routing

| `environment` param | Login DB used |
|---|---|
| `prod` | `Login` |
| `dev` | `Login-dev` |
| `uat` | `Login-uat` |
| `test` | `Login-test` |

---

## 📦 Dependencies

```
fastapi / uvicorn   — API server
pymupdf             — reads PDF drawings
reportlab           — generates PDF reports
openpyxl            — generates Excel summary
pyodbc              — SQL Server connection
```

Install: `pip install -r requirements.txt`

---
=======
# Hvl_surface_calculation_API
HVL Surface API is a production-grade FastAPI service that eliminates manual data entry in industrial painting operations. It accepts ZIP files containing PDFs, emails, and Outlook messages — extracts structured coating data using Claude AI — and persists complete orders into SQL Server in a single atomic transaction.

