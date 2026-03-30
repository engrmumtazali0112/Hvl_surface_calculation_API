<<<<<<< HEAD

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
=======
<div align="center">

<!-- Animated Header -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=200&section=header&text=HVL%20Surface%20API&fontSize=60&fontColor=fff&animation=twinkling&fontAlignY=35&desc=AI-Powered%20Painting%20Quotation%20Processing%20Pipeline&descAlignY=60&descSize=18" width="100%"/>

<!-- Animated Typing -->
<a href="https://git.io/typing-svg">
  <img src="https://readme-typing-svg.herokuapp.com?font=JetBrains+Mono&weight=700&size=22&pause=1000&color=00D9FF&center=true&vCenter=true&width=700&lines=ZIP+%E2%86%92+AI+Extraction+%E2%86%92+SQL+Server+%E2%9C%85;PDF+%2B+EML+%2B+MSG+%E2%86%92+Structured+Orders;Claude+AI+%2B+FastAPI+%2B+pyodbc;One+ZIP+%3D+One+Order.+Always.;Industrial+ERP+Automation+%F0%9F%8F%AD" alt="Typing SVG" />
</a>

<br/>

<!-- Badges Row 1 -->
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-2.0-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Claude AI](https://img.shields.io/badge/Claude%20AI-Anthropic-CC785C?style=for-the-badge&logo=anthropic&logoColor=white)](https://anthropic.com)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-pyodbc-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)](https://microsoft.com/sql-server)

<!-- Badges Row 2 -->
[![Azure DevOps](https://img.shields.io/badge/Azure%20DevOps-Task%202047-0078D7?style=for-the-badge&logo=azuredevops&logoColor=white)](https://dev.azure.com/Virevo/HVL_Cost_estimation)
[![OpenAPI](https://img.shields.io/badge/OpenAPI-3.1-6BA539?style=for-the-badge&logo=openapiinitiative&logoColor=white)](#api-reference)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=for-the-badge)](#)

<br/>

<!-- Stats Row -->
![GitHub stars](https://img.shields.io/github/stars/engrmumtazali0112/hvl_surface?style=social)
![GitHub forks](https://img.shields.io/github/forks/engrmumtazali0112/hvl_surface?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/engrmumtazali0112/hvl_surface?style=social)

</div>

---

## 📌 Table of Contents

<div align="center">

| Section | Description |
|:-------:|:-----------|
| [🚀 Overview](#-overview) | What this project does and why |
| [⚡ Pipeline Flow](#-pipeline-flow) | End-to-end processing architecture |
| [📁 Project Structure](#-project-structure) | Full directory layout explained |
| [🔧 Setup & Installation](#-setup--installation) | Get running in minutes |
| [🌍 Environment Variables](#-environment-variables) | Configuration reference |
| [📡 API Reference](#-api-reference) | Endpoints, inputs, outputs |
| [📥 Input / 📤 Output](#-input--output) | What goes in, what comes out |
| [🗄️ Database Mapping](#️-database-mapping) | Tables, parameters, environments |
| [💡 Business Rules](#-business-rules) | Core logic explained |
| [🧪 Testing](#-testing) | Running tests and verification |
| [📊 Live Results](#-live-results) | Real extraction examples |

</div>

---

## 🚀 Overview

<div align="center">
<img src="https://img.shields.io/badge/One%20ZIP-One%20Order-FF6B35?style=flat-square&labelColor=1a1a2e" />
<img src="https://img.shields.io/badge/AI%20%2B%20Regex-Dual%20Extraction-00D9FF?style=flat-square&labelColor=1a1a2e" />
<img src="https://img.shields.io/badge/Atomic-Transaction-7C3AED?style=flat-square&labelColor=1a1a2e" />
</div>

<br/>

> **HVL Surface API** is a production-grade FastAPI service that eliminates manual data entry in industrial painting operations. It accepts ZIP files containing PDFs, emails, and Outlook messages — extracts structured coating data using Claude AI — and persists complete orders into SQL Server in a single atomic transaction.

```
Problem  →  ZIP files with PDFs + emails arrive manually
Solution →  Upload ZIP → AI extracts everything → DB gets perfect records
Result   →  Zero manual entry. Zero duplicates. Every time.
```

### ✨ Key Highlights

- 🤖 **Claude AI + Regex Fallback** — AI-first extraction, regex as safety net. Never silently fails.
- 🔒 **Atomic Transactions** — All DB operations commit together or roll back completely.
- 🔍 **Smart Deduplication** — Fuzzy customer matching with legal suffix normalization (SRL, SPA, GmbH…).
- 🌐 **Multi-Environment** — `prod` / `dev` / `uat` / `test` routing from a single codebase.
- 📐 **Surface Area Calculator** — Weight-based, dimension-based, and area-based methods per part type.

---

## ⚡ Pipeline Flow

```
╔══════════════════════════════════════════════════════════════════╗
║              POST /api/v1/process  (multipart/form-data)         ║
║           zip_file  ·  company_id  ·  environment                ║
╚══════════════════╦═══════════════════════════════════════════════╝
                   ║
                   ▼
        ┌──────────────────────┐
        │  1. Resolve DB        │  Login / Login-dev / Login-uat / Login-test
        │     (Login DB lookup) │  SELECT DbName FROM Companies WHERE Id = ?
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────────────────────────────┐
        │  2. Extract Text from ZIP                     │
        │                                               │
        │   ┌──────────┐  ┌──────────┐  ┌──────────┐  │
        │   │  .pdf    │  │  .eml    │  │  .msg    │  │
        │   │pdfplumber│  │  email   │  │  binary  │  │
        │   │          │  │  parser  │  │  scan    │  │
        │   └──────────┘  └──────────┘  └──────────┘  │
        └──────────────────────┬───────────────────────┘
                               │  merged text
                               ▼
        ┌──────────────────────────────────────────────┐
        │  3. Claude AI Extraction                      │
        │                                               │
        │  → customer name & identity code              │
        │  → article codes & descriptions               │
        │  → RAL color · finishing · batch · pitch      │
        │  → protections type & presence                │
        │  → total painting surface area                │
        └──────────────────────┬───────────────────────┘
                               │
               ╔═══════════════╩═══════════════╗
               ║   Company SQL Server DB        ║
               ║   (single atomic transaction)  ║
               ╠════════════════════════════════╣
               ║                                ║
               ║  4. Upsert Customer            ║
               ║     └─ LIKE match or insert    ║
               ║                                ║
               ║  5. Upsert Items               ║
               ║     └─ ItemCode exact match    ║
               ║                                ║
               ║  6. Insert Order               ║
               ║     └─ OFF-HVL-YYYY-NNNNNN     ║
               ║                                ║
               ║  7. Insert OrderRow(s)          ║
               ║     └─ one row per item         ║
               ║                                ║
               ║  8. Insert OrderValues         ║
               ║     └─ params 27–33 always     ║
               ╚════════════════════════════════╝
                               │
                               ▼
              ┌────────────────────────────────┐
              │  200 OK                        │
              │  {                             │
              │    "NOrder":     "OFF-HVL-...",│
              │    "IdOrder":    485603,       │
              │    "IdCustomer": 2994,         │
              │    "items":      [...],        │
              │    "company_db": "Demo_Etwin"  │
              │  }                             │
              └────────────────────────────────┘
```
>>>>>>> 228bcb25672ae0256f3911f8e1a2abf4840228a3

---

## 📁 Project Structure

```
<<<<<<< HEAD
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
=======
📦 Hvl_Surface_Calculation/
│
├── 📂 api/                          # FastAPI route definitions
│   ├── __init__.py
│   └── routes.py                    # POST /api/v1/process endpoint
│
├── 📂 core/                         # Orchestration layer
│   └── processor.py                 # Per-file extraction orchestrator
│
├── 📂 db/                           # Database layer
│   ├── __init__.py
│   ├── connection.py                # pyodbc connection helpers
│   ├── operations.py                # All INSERT / UPDATE / SELECT logic
│   └── repository.py                # Data access patterns
│
├── 📂 01_inputs/                    # 📥 Drop ZIP files here (watched folder)
├── 📂 02_outputs/                   # 📤 Generated reports and summaries
├── 📂 03_archive/                   # 🗃️  Processed ZIPs moved here
│
├── 🐍 main.py                       # FastAPI app entry point
├── 🐍 config.py                     # DB config + environment mapping
├── 🐍 extractor.py                  # Lightweight extractor (AI + regex)
├── 🐍 hvl_extractor.py              # Full extractor with surface area calc
├── 🐍 hvl_api.py                    # Legacy API helper module
├── 🐍 hvl_batch.py                  # Batch processor (folder-watch mode)
├── 🐍 verify_db.py                  # DB schema verification utility
│
├── 📄 requirements.txt              # pip dependencies
├── 📄 pyproject.toml                # Poetry project config
├── 📄 poetry.lock                   # Locked dependency tree
├── 📄 .env                          # 🔒 Credentials (not committed)
├── 📄 .gitignore
└── 📄 README.md
>>>>>>> 228bcb25672ae0256f3911f8e1a2abf4840228a3
```

---

<<<<<<< HEAD
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

=======
## 🔧 Setup & Installation

### Prerequisites

```bash
# Required
Python 3.10+
ODBC Driver 17 for SQL Server
Anthropic API key (Claude)
```

### Option A — pip (quick start)

```bash
# 1. Clone the repository
git clone https://github.com/engrmumtazali0112/hvl_surface.git
cd hvl_surface

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate       # Linux / macOS
.venv\Scripts\activate          # Windows PowerShell

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy and edit environment file
copy .env.example .env           # Windows
cp .env.example .env             # Linux / macOS
# → Edit .env with your credentials (see below)

# 5. Start the server
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Option B — Poetry

```bash
git clone https://github.com/engrmumtazali0112/hvl_surface.git
cd hvl_surface
poetry install
poetry run uvicorn main:app --reload --port 8000
```

### ✅ Verify Installation

```bash
curl http://localhost:8000/api/v1/health
# Expected: {"status": "ok"}
```

Open **http://127.0.0.1:8000/docs** for the interactive Swagger UI.

---

## 🌍 Environment Variables

Create a `.env` file in the project root:

```env
# ── SQL Server ──────────────────────────────────────
SQL_SERVER=your-server-host-or-ip
SQL_USER=sa
SQL_PASS=your-password

# ── AI Keys ─────────────────────────────────────────
ANTHROPIC_API_KEY=sk-ant-...          # Required for Claude AI extraction
GEMINI_API_KEY=                        # Optional fallback
OPENAI_API_KEY=                        # Optional fallback

# ── App Config ──────────────────────────────────────
DEFAULT_ENVIRONMENT=dev
MODIFIER_USER=hvl_api
```

| Variable | Required | Description |
|---|:---:|---|
| `SQL_SERVER` | ✅ | SQL Server hostname or IP |
| `SQL_USER` | ✅ | SQL login username |
| `SQL_PASS` | ✅ | SQL login password |
| `ANTHROPIC_API_KEY` | ✅ | Anthropic Claude API key |
| `GEMINI_API_KEY` | ➖ | Google Gemini (optional fallback) |
| `OPENAI_API_KEY` | ➖ | OpenAI (optional fallback) |
| `MODIFIER_USER` | ➖ | Username stamped on DB records (default: `hvl_api`) |

---

## 📡 API Reference

### `POST /api/v1/process`

Processes a ZIP file and creates a full order in the database.

**Content-Type:** `multipart/form-data`

| Field | Type | Required | Description |
|---|---|:---:|---|
| `file` | ZIP file | ✅ | ZIP containing PDFs and/or `.eml` / `.msg` / `.txt` |
| `company_id` | `int` | ✅ | IdCompany from `Login.[dbo].Companies` |
| `environment` | `string` | ✅ | `prod` / `dev` / `uat` / `test` |
| `modifier_user` | `string` | ➖ | Username for audit trail (default: `hvl_api`) |
| `claude_key` | `string` | ➖ | Override Anthropic API key per-request |

**Success Response `200 OK`:**

```json
{
  "status":          "ok",
  "company_db":      "Demo_Etwin-Dev",
  "environment":     "dev",
  "files_processed": 3,
  "files_failed":    0,
  "norders":         ["OFF-HVL-2026-000035", "OFF-HVL-2026-000036"],
  "processed": [
    {
      "file":        "Rdo 377-25 Franke.eml",
      "status":      "ok",
      "NOrder":      "OFF-HVL-2026-000035",
      "IdOrder":     485592,
      "IdOrderRow":  426705,
      "IdItem":      825698,
      "IdCustomer":  2994
    }
  ],
  "errors": []
}
```

**Example — curl:**

```bash
curl -X POST http://localhost:8000/api/v1/process \
  -F "file=@Input.zip" \
  -F "company_id=1" \
  -F "environment=dev"
```

**Example — Python:**

```python
import requests

with open("Input.zip", "rb") as f:
    response = requests.post(
        "http://localhost:8000/api/v1/process",
        files={"file": ("Input.zip", f, "application/zip")},
        data={"company_id": 1, "environment": "dev"}
    )

print(response.json())
```

---

### `GET /api/v1/health`

```bash
curl http://localhost:8000/api/v1/health
# {"status": "ok"}
```

---

## 📥 Input / 📤 Output

### 📥 Input — What goes into the ZIP

```
Input.zip
├── quotation_377-25.pdf          ← Main painting quotation PDF
├── Rdo_Franke.eml                ← Customer email with attachments
├── Offerta_IMMERGAS.msg          ← Outlook message with embedded PDF
└── specs_addendum.pdf            ← Additional spec sheet (optional)
```

> ☝️ **One ZIP = One Order.** All files are merged into a single structured result regardless of how many documents are inside.

### What the extractor reads from documents

| Field | Example | Source |
|---|---|---|
| Article code | `560.0755.201` | PDF header / email body |
| Article description | `GENERICO GR. FIANCO MANTELLO` | Document text |
| Customer name | `IMMERGAS` | Letterhead / email From |
| Customer VAT / fiscal | `IT01234567890` | Document footer |
| RAL color | `RAL 9010` | Coating specs |
| Finishing type | `low gloss` | Coating specs |
| Batch size | `500 pcs` | Order quantities |
| Pitch | `2.5 mm` | Technical drawing |
| Protections presence | `yes` | Notes section |
| Protections type | `masking tape` | Notes section |
| Surface area | `0.7222 m²` | Calculated or stated |

### 📤 Output — What gets written to the database

```
[Login DB]
  └─ Companies  →  resolves DbName for company_id

[Company DB]  ← single atomic transaction
  ├─ Customers      (upsert — COALESCE logic, no overwrites)
  ├─ Items          (upsert — match on ItemCode)
  ├─ Orders         (insert — NOrder = OFF-HVL-YYYY-NNNNNN)
  ├─ OrderRows      (insert — IdOrderType=2, IdOrderStates=7)
  └─ OrderValues    (insert — 7 parameters always written)
       ├─ param 27  →  RAL color
       ├─ param 28  →  Finishing type
       ├─ param 29  →  Batch size
       ├─ param 30  →  Pitch
       ├─ param 31  →  Presence of protections
       ├─ param 32  →  Type of protections
       └─ param 33  →  Total painting surface area
```

### 📂 File outputs (batch mode)

Running `hvl_batch.py` also produces:

| File | Location | Description |
|---|---|---|
| `HVL_Summary.xlsx` | `02_outputs/` | Excel summary of all processed items |
| `README_report.pdf` | `02_outputs/` | PDF extraction report |
| Processed ZIPs | `03_archive/` | Input ZIPs moved after processing |

---

## 🗄️ Database Mapping

### Environment → Login DB

| `environment` | Login DB used |
|:---:|---|
| `prod` | `Login` |
| `dev` | `Login-Dev` |
| `uat` | `Login-Uat` |
| `test` | `Login-Test` |

### OrderValues Parameters

| IdOrderParameter | Field name | Default if not found |
|:---:|---|:---:|
| 27 | RAL color | `"0"` |
| 28 | Finishing type | `"0"` |
| 29 | Batch size | `"0"` |
| 30 | Pitch | `"0"` |
| 31 | Presence of protections | `"0"` |
| 32 | Type of protections | `"0"` |
| 33 | Total painting surface area | `"0"` |

### Company IDs (prod)

| IdCompany | Database |
|:---:|---|
| 1 | Virevo_Etwin |
| 2 | DavCoilSrl_Etwin |
| 3 | Extrema_Etwin |
| 4 | ErediAiroldi_Etwin |
| 6 | Playmec_Etwin |
| 7 | NovaSidera_Etwin |
| 8 | Coma_Etwin |
| 10 | FlySolarTech_Etwin |
| 12 | Demo_Etwin |
| 15 | Hvl_Etwin |

---

## 💡 Business Rules

```
1. ONE ZIP = ONE ORDER
   All documents inside a ZIP are merged into a single extraction result.
   One order, one NOrder, one transaction.

2. CUSTOMER MATCHING
   Step 1 → exact match on CustomerIdentityCode (VAT / fiscal code)
   Step 2 → LIKE '%normalized_name%' (strips SRL, SPA, GmbH, Ltd…)
   Step 3 → no match found → insert new customer
   Step 4 → match found → update empty fields only (COALESCE), reuse ID

3. ITEM MATCHING
   Exact match on ItemCode.
   If found → update description/revision if changed.
   If not found → insert new item.

4. NORDER SEQUENCE
   Format: OFF-HVL-{YEAR}-{6-digit-padded}
   Generated by: SELECT MAX(NOrder) WHERE NOrder LIKE 'OFF-HVL-{year}-%'
   Thread-safe within the transaction.

5. ORDERROW
   IdOrderType = 2  (fixed)
   IdOrderStates = 7  (fixed)
   OrderRow = 1  (fixed)
   One row inserted per item detected in the documents.

6. ORDERVALUES
   All 7 parameters (27–33) are always written.
   Value = extracted string, or "0" if not found — no silent nulls.

7. AUDIT TRAIL
   ModifierUser = "hvl_api" on every insert/update (configurable).
```

---

## 🧪 Testing

### Health check

```bash
curl http://localhost:8000/api/v1/health
```

### Test with a real ZIP

```bash
curl -X POST http://localhost:8000/api/v1/process \
  -F "file=@01_inputs/test_offer.zip" \
  -F "company_id=12" \
  -F "environment=dev" \
  | python -m json.tool
```

### Verify database schema

```bash
python verify_db.py
```

### Run batch mode (folder watch)

```bash
# Drop ZIP files into 01_inputs/ then run:
python hvl_batch.py
```

Expected terminal output:
```
Processing: 3.037280.pdf
>> Reading PDF …
OK  Text extracted — 4,664 chars
!>  Using regex extraction

EXTRACTION RESULT
Part Type         SHEET_METAL
Client            IMMERGAS
RAL Color         RAL9010
Finishing         low gloss
Material          DC01
Sheet Thickness   0.7 mm
Weight            1.959 kg

SURFACE AREA : 0.7222120 m²
Method       : SHEET_METAL — weight-based
Calculation  : (1.959 kg ÷ (7750 kg/m³ × 0.7 mm)) × 2 = 0.7222 m²
Confidence   : MEDIUM
```

### Fix database on first run

```bash
# If you see connection errors on first run:
python fix_all.py
# Fixes case mismatches in Login.Companies.DbName
# Creates missing tables in company DBs
```

---

## 📊 Live Results

Real DB log output from a production test run (`2026-03-30`):

```
2026-03-30 16:21:02  INFO  db.operations | Inserted OrderRow id=426732 (Order=485607, Item=825700, Customer=2993)
2026-03-30 16:21:07  INFO  db.operations | OrderValue param=27 (ral_color) = '0'
2026-03-30 16:21:07  INFO  db.operations | OrderValue param=28 (finishing_type) = '0'
2026-03-30 16:21:16  INFO  hvl_api       | ✓ OrderRow 426732 for item 825700
2026-03-30 16:21:27  INFO  db.operations | Inserted OrderRow id=426733 (Order=485607, Item=825698, Customer=2993)
2026-03-30 16:21:30  INFO  db.operations | OrderValue param=27 (ral_color) = 'RAL 9005'
2026-03-30 16:21:41  INFO  hvl_api       | ✓ OrderRow 426733 for item 825698
INFO:     127.0.0.1 - "POST /api/v1/process HTTP/1.1" 200 OK
```

---

## 🛠️ Tech Stack

<div align="center">

| Layer | Technology |
|---|---|
| **API Framework** | FastAPI 2.0 + Uvicorn |
| **AI Extraction** | Anthropic Claude (claude-3-5-sonnet) |
| **PDF Parsing** | pdfplumber |
| **Email Parsing** | Python `email` stdlib + extract-msg |
| **Database** | Microsoft SQL Server via pyodbc |
| **Config** | python-dotenv |
| **Packaging** | Poetry + pip |
| **CI / Project** | Azure DevOps |
| **Docs** | OpenAPI 3.1 / Swagger UI |

</div>

---

## 🤝 Contributing

```bash
# 1. Fork the repo
# 2. Create your feature branch
git checkout -b feature/amazing-improvement

# 3. Commit your changes
git commit -m "feat: add amazing improvement"

# 4. Push and open a Pull Request
git push origin feature/amazing-improvement
```

---

## 👤 Author

<div align="center">

**Mumtaz Ali**

[![GitHub](https://img.shields.io/badge/GitHub-engrmumtazali0112-181717?style=for-the-badge&logo=github)](https://github.com/engrmumtazali0112?tab=repositories)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/mumtazali12/)
[![Azure DevOps](https://img.shields.io/badge/Azure%20DevOps-Virevo-0078D7?style=for-the-badge&logo=azuredevops)](https://dev.azure.com/Virevo/HVL_Cost_estimation)

*Built with ❤️ at the intersection of AI and industrial ERP automation.*

</div>

---

<div align="center">

<!-- Footer wave -->
<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=100&section=footer" width="100%"/>

**⭐ If this project helped you, please give it a star!**

`Python` · `FastAPI` · `Claude AI` · `SQL Server` · `Industrial Automation`

</div>
>>>>>>> 228bcb25672ae0256f3911f8e1a2abf4840228a3
