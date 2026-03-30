"""
verify_db.py — Run from terminal to inspect all records inserted by the API.

Usage:
    python verify_db.py [company_db] [environment]

Examples:
    python verify_db.py Demo_Etwin-dev dev
    python verify_db.py DavcoilSrl_Etwin-dev dev
"""
import sys
import os
import pyodbc

# ── Connection settings ───────────────────────────────────────────────────
SERVER   = os.getenv("DB_SERVER",   "host8728.shserver.it")
PORT     = int(os.getenv("DB_PORT", "1438"))
USER     = os.getenv("DB_USER",     "sa")
PASSWORD = os.getenv("DB_PASSWORD", "NmlILt68qnWCQT7Eog")
TIMEOUT  = 60

COMPANY_DB = sys.argv[1] if len(sys.argv) > 1 else "Demo_Etwin-dev"
ENV        = sys.argv[2] if len(sys.argv) > 2 else "dev"

# ── Driver detection ──────────────────────────────────────────────────────
PREFERRED = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "SQL Server",
]
driver = next((d for d in PREFERRED if d in pyodbc.drivers()), None)
if not driver:
    print("ERROR: No SQL Server ODBC driver found.")
    sys.exit(1)


def connect(database: str) -> pyodbc.Connection:
    cs = (
        f"DRIVER={{{driver}}};"
        f"SERVER={{{SERVER},{PORT}}};"
        f"DATABASE={database};"
        f"UID={USER};PWD={PASSWORD};"
        f"Connection Timeout={TIMEOUT};"
        f"TrustServerCertificate=yes;Encrypt=yes;"
    )
    conn = pyodbc.connect(cs, timeout=TIMEOUT)
    conn.autocommit = True
    return conn


def query(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return cols, rows


def print_table(title, cols, rows, max_col=35):
    print(f"\n{'='*70}")
    print(f"  {title}  ({len(rows)} row{'s' if len(rows) != 1 else ''})")
    print(f"{'='*70}")
    if not rows:
        print("  (no rows)")
        return
    widths = {c: min(max_col, max(len(c), max(len(str(r.get(c) or "")) for r in rows))) for c in cols}
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep    = "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for row in rows:
        print(" | ".join(str(row.get(c) or "").ljust(widths[c])[:max_col] for c in cols))


# ── Main ──────────────────────────────────────────────────────────────────
print(f"\nConnecting to  : {SERVER},{PORT}")
print(f"Company DB     : {COMPANY_DB}")
print(f"Environment    : {ENV}")

try:
    conn = connect(COMPANY_DB)
    print(f"✓ Connected to {COMPANY_DB}\n")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    sys.exit(1)

# 1. Items
cols, rows = query(conn, """
    SELECT TOP 20 [IdItem],[ItemCode],[ItemDescription],[Revision],[ModifierUser],[CreationDate]
    FROM [dbo].[Items]
    ORDER BY [IdItem] DESC
""")
print_table("ITEMS (latest 20)", cols, rows)

# 2. Customers
cols, rows = query(conn, """
    SELECT TOP 20 [IdCustomer],[CustomerIdentityCode],[BusinessName],[Email],[TelephoneNumber],[ModifierUser],[CreationDate]
    FROM [dbo].[Customers]
    ORDER BY [IdCustomer] DESC
""")
print_table("CUSTOMERS (latest 20)", cols, rows)

# 3. Orders
cols, rows = query(conn, """
    SELECT TOP 20 [Id],[NOrder],[Note],[ModifierUser],[CreationDate]
    FROM [dbo].[Orders]
    ORDER BY [Id] DESC
""")
print_table("ORDERS (latest 20)", cols, rows)

# 4. OrderRows
cols, rows = query(conn, """
    SELECT TOP 20 or2.[IdOrderRow], o.[NOrder], or2.[IdItem], or2.[IdCustomer],
           or2.[IdOrderType], or2.[IdOrderStates], or2.[OrderRow],
           or2.[ModifierUser], or2.[CreationDate]
    FROM [dbo].[OrderRows] or2
    JOIN [dbo].[Orders]    o  ON o.[Id] = or2.[IdOrderParent]
    ORDER BY or2.[IdOrderRow] DESC
""")
print_table("ORDER ROWS (latest 20)", cols, rows)

# 5. OrderValues
cols, rows = query(conn, """
    SELECT TOP 40 ov.[IdOrderValues], o.[NOrder], ov.[IdOrderRow],
           ov.[IdOrderParameter], ov.[Value], ov.[ModifierUser], ov.[CreationDate]
    FROM [dbo].[OrderValues]  ov
    JOIN [dbo].[OrderRows]    orr ON orr.[IdOrderRow]    = ov.[IdOrderRow]
    JOIN [dbo].[Orders]       o   ON o.[Id]              = orr.[IdOrderParent]
    ORDER BY ov.[IdOrderValues] DESC
""")
print_table("ORDER VALUES (latest 40)", cols, rows)

# 6. Full join summary — one line per NOrder
print(f"\n{'='*70}")
print("  FULL PIPELINE SUMMARY (latest 10 orders)")
print(f"{'='*70}")
cols, rows = query(conn, """
    SELECT TOP 10
        o.[NOrder],
        i.[ItemCode],
        i.[ItemDescription],
        c.[BusinessName],
        c.[Email],
        orr.[IdOrderStates],
        o.[CreationDate]
    FROM [dbo].[Orders]    o
    JOIN [dbo].[OrderRows] orr ON orr.[IdOrderParent] = o.[Id]
    JOIN [dbo].[Items]     i   ON i.[IdItem]          = orr.[IdItem]
    JOIN [dbo].[Customers] c   ON c.[IdCustomer]      = orr.[IdCustomer]
    ORDER BY o.[Id] DESC
""")
print_table("", cols, rows, max_col=40)

conn.close()
print("\n✓ Verification complete.\n")