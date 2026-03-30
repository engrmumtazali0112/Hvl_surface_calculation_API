# HVL Surface API — Task 2047

Accepts a ZIP file + company context, runs the HVL extractor (Task 2014) on
every PDF/EML inside, and inserts the results into the company's SQL Server DB.

---

## Install

```powershell
python -m pip install -r requirements_api.txt
```

Make sure `hvl_extractor.py` and `hvl_batch.py` (Task 2014) are in the same folder.

---

## Configure DB connection

Set these environment variables (or edit `.env`):

```env
DB_SERVER   = your-sql-server-host
DB_PORT     = 1433
DB_USER     = sa
DB_PASSWORD = your-password
DB_DRIVER   = SQL Server
```

---

## Run

```powershell
uvicorn hvl_api:app --reload --port 8000
```

Swagger UI: http://localhost:8000/docs

---

## API Endpoint

### `POST /api/v1/process`

**Form fields:**

| Field           | Required | Description |
|-----------------|----------|-------------|
| `file`          | ✅       | ZIP file containing PDFs / EML / TXT |
| `company_id`    | ✅       | IdCompany from `[Login].[dbo].[Companies]` |
| `environment`   | ✅       | `prod` / `dev` / `uat` / `test` |
| `modifier_user` | ❌       | Written to all `ModifierUser` columns (default: `hvl_api`) |
| `claude_key`    | ❌       | Anthropic API key for best accuracy |
| `gemini_key`    | ❌       | Google Gemini key |
| `openai_key`    | ❌       | OpenAI key |

**Example with curl:**

```bash
curl -X POST http://localhost:8000/api/v1/process \
  -F "file=@Dati_AI_HVL.zip" \
  -F "company_id=42" \
  -F "environment=dev" \
  -F "modifier_user=mumtaz"
```

**Response:**

```json
{
  "status": "ok",
  "company_id": 42,
  "company_db": "CompanyXYZ",
  "environment": "dev",
  "files_processed": 3,
  "files_skipped": [],
  "norders": [
    "OFF-HVL-2026-000001",
    "OFF-HVL-2026-000002",
    "OFF-HVL-2026-000003"
  ],
  "orders": [
    {
      "source_file": "560.0755.201.pdf",
      "NOrder": "OFF-HVL-2026-000001",
      "IdOrder": 1,
      "IdOrderRow": 1,
      "IdItem": 5,
      "IdCustomer": 3,
      "article_code": "560.0755.201",
      "client_name": "Franke Kaffeemaschinen AG",
      "total_surface_area_m2": 0.0570721,
      "ral_color": "RAL9005",
      "finishing_type": "matte",
      "batch_size": 1000
    }
  ]
}
```

---

## DB Flow (per file)

```
ZIP
 └─ PDF / EML / TXT
       │
       ▼
  HVL Extractor (Task 2014)
       │  article_code, client_name, RAL, finishing,
       │  batch_size, pitch, protections, surface_area_m²
       ▼
  [dbo].[Items]        ← upsert on ItemCode
  [dbo].[Customers]    ← upsert on CustomerIdentityCode
  [dbo].[Orders]       ← insert  NOrder = OFF-HVL-YYYY-NNNNNN
  [dbo].[OrderRows]    ← insert  IdOrderType=2, IdOrderStates=7
  [dbo].[OrderValues]  ← insert  params 27–33 (RAL→surface area)
       │
       ▼
  Returns NOrder
```

---

## Environment → Login DB mapping

| environment | Login DB used |
|-------------|---------------|
| `prod`      | `[Login]`     |
| `dev`       | `[Login-dev]` |
| `uat`       | `[Login-uat]` |
| `test`      | `[Login-test]`|
