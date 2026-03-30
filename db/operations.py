"""
db/operations.py — All database operations for the HVL Surface pipeline.

KEY FIXES (per Mojtaba's review):
  - find_or_create_customer: searches by BusinessName first, never duplicates
  - insert_order_values: ALL params 27–33 always written; missing values default to "0"
  - One Order per ZIP (orchestrated by routes.py)

Pipeline order (called from routes.py):
  1. upsert_item              → returns IdItem         (one per file/extraction)
  2. find_or_create_customer  → returns IdCustomer     (ONE per ZIP, smart match)
  3. insert_order             → returns (Id, NOrder)   (ONE per ZIP)
  4. insert_order_row         → returns IdOrderRow     (one per item)
  5. insert_order_values      → params 27–33           (one set per OrderRow)
"""
import logging
import re
from datetime import datetime
from typing import Optional, Tuple, List

import pyodbc

logger = logging.getLogger("db.operations")

# ---------------------------------------------------------------------------
# OrderValues parameter mapping (params 27–33, always written)
# ---------------------------------------------------------------------------
ORDER_VALUE_PARAMS = {
    27: "ral_color",
    28: "finishing_type",
    29: "batch_size",
    30: "pitch",
    31: "presence_of_protections",
    32: "type_of_protections",
    33: "total_painting_surface",
}


# ---------------------------------------------------------------------------
# 1. Items — upsert by ItemCode
# ---------------------------------------------------------------------------

def upsert_item(conn: pyodbc.Connection, data: dict, modifier_user: str) -> int:
    """
    Insert or update [dbo].[Items]. Matches on ItemCode.
    Returns IdItem.
    """
    code: Optional[str] = data.get("ItemCode") or data.get("item_code")

    if not code:
        desc = (
            data.get("ItemDescription")
            or data.get("item_description")
            or data.get("filename")
            or ""
        )
        if desc:
            code = re.sub(r"[^A-Za-z0-9\-_.]", "_", desc.strip())[:50]
            logger.warning(f"ItemCode was None — using derived code: {code!r}")
        else:
            raise ValueError("Cannot upsert item: ItemCode is None and no fallback available.")

    code        = code[:100]
    description = (data.get("ItemDescription") or data.get("item_description") or "")[:500] or None
    revision    = (data.get("Revision")         or data.get("revision")         or "")[:50]  or None
    now         = datetime.utcnow()

    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT [IdItem] FROM [dbo].[Items] WHERE [ItemCode] = ?", (code,)
        )
        row = cursor.fetchone()

        if row:
            id_item = row[0]
            cursor.execute(
                """
                UPDATE [dbo].[Items]
                SET [ItemDescription] = ?,
                    [Revision]        = ?,
                    [ModifierUser]    = ?,
                    [ModifierDate]    = ?
                WHERE [IdItem] = ?
                """,
                (description, revision, modifier_user, now, id_item),
            )
            logger.info(f"Updated Item id={id_item}, code={code!r}")
        else:
            cursor.execute(
                """
                INSERT INTO [dbo].[Items]
                    ([ItemCode],[ItemDescription],[Revision],[CreationDate],[ModifierUser],[ModifierDate])
                OUTPUT INSERTED.[IdItem]
                VALUES (?,?,?,?,?,?)
                """,
                (code, description, revision, now, modifier_user, now),
            )
            inserted = cursor.fetchone()
            if not inserted:
                raise Exception(f"Could not retrieve inserted Item ID for code: {code!r}")
            id_item = inserted[0]
            logger.info(f"Inserted Item id={id_item}, code={code!r}")

        conn.commit()
        return id_item

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# 2. Customers — smart find-or-create (never blindly insert)
# ---------------------------------------------------------------------------

def find_or_create_customer(conn: pyodbc.Connection, data: dict, modifier_user: str) -> int:
    """
    Find an existing customer by BusinessName (case-insensitive, trimmed).
    If not found, try CustomerIdentityCode / email domain.
    Only inserts a NEW customer if absolutely no match is found.
    Returns IdCustomer.
    """
    business_name = (
        data.get("BusinessName")
        or data.get("business_name")
        or data.get("sender_name")
        or data.get("from_name")
    )
    identity_code = (
        data.get("CustomerIdentityCode")
        or data.get("customer_identity_code")
        or data.get("customer_code")
    )
    email = (
        data.get("Email")
        or data.get("email")
        or data.get("sender_email")
    )

    cursor = conn.cursor()
    now = datetime.utcnow()

    try:
        # ── Step 1: Match by BusinessName (fuzzy, case-insensitive) ──────────
        if business_name:
            cursor.execute(
                """
                SELECT TOP 1 [IdCustomer], [BusinessName]
                FROM [dbo].[Customers]
                WHERE LOWER(LTRIM(RTRIM([BusinessName]))) = LOWER(LTRIM(RTRIM(?)))
                   OR LOWER([BusinessName]) LIKE LOWER(?)
                """,
                (business_name, f"%{business_name.split()[0]}%"),
            )
            row = cursor.fetchone()
            if row:
                id_customer = row[0]
                logger.info(f"Customer found by name match: id={id_customer}, name={row[1]!r}")
                _update_customer_fields(cursor, id_customer, data, modifier_user, now)
                conn.commit()
                return id_customer

        # ── Step 2: Match by CustomerIdentityCode ────────────────────────────
        if identity_code:
            cursor.execute(
                "SELECT [IdCustomer] FROM [dbo].[Customers] WHERE [CustomerIdentityCode] = ?",
                (identity_code,),
            )
            row = cursor.fetchone()
            if row:
                id_customer = row[0]
                logger.info(f"Customer found by identity code: id={id_customer}")
                _update_customer_fields(cursor, id_customer, data, modifier_user, now)
                conn.commit()
                return id_customer

        # ── Step 3: Match by email domain ────────────────────────────────────
        if email:
            domain = email.split("@")[-1] if "@" in email else ""
            if domain:
                cursor.execute(
                    "SELECT TOP 1 [IdCustomer] FROM [dbo].[Customers] WHERE [Email] LIKE ?",
                    (f"%@{domain}",),
                )
                row = cursor.fetchone()
                if row:
                    id_customer = row[0]
                    logger.info(f"Customer found by email domain: id={id_customer}, domain={domain}")
                    _update_customer_fields(cursor, id_customer, data, modifier_user, now)
                    conn.commit()
                    return id_customer

        # ── Step 4: No match found — insert new customer ──────────────────────
        if not identity_code:
            if email:
                domain = email.split("@")[-1].split(".")[0].upper() if "@" in email else "UNK"
                identity_code = f"CUST-{domain}"
            elif business_name:
                import hashlib
                identity_code = "CUST-" + hashlib.md5(business_name.encode()).hexdigest()[:8].upper()
            else:
                identity_code = f"CUST-UNKNOWN-{now.strftime('%Y%m%d%H%M%S')}"

        cursor.execute(
            """
            INSERT INTO [dbo].[Customers]
                ([CustomerIdentityCode],[BusinessName],[Address],[Cap],[City],
                 [Province],[Country],[TelephoneNumber],[Fax],[VatNumber],
                 [FiscalCode],[Note],[Email],[CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[IdCustomer]
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                identity_code,
                business_name,
                data.get("Address") or data.get("address"),
                data.get("Cap")     or data.get("cap"),
                data.get("City")    or data.get("city"),
                data.get("Province") or data.get("province"),
                data.get("Country") or data.get("country"),
                data.get("TelephoneNumber") or data.get("telephone"),
                data.get("Fax")     or data.get("fax"),
                data.get("VatNumber") or data.get("vat_number"),
                data.get("FiscalCode") or data.get("fiscal_code"),
                data.get("Note")    or data.get("note"),
                email,
                now, modifier_user, now,
            ),
        )
        inserted = cursor.fetchone()
        if not inserted:
            raise Exception(f"Could not retrieve inserted Customer ID for code: {identity_code!r}")
        id_customer = inserted[0]
        logger.info(f"Inserted NEW Customer id={id_customer}, code={identity_code!r}, name={business_name!r}")
        conn.commit()
        return id_customer

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _update_customer_fields(cursor, id_customer: int, data: dict, modifier_user: str, now: datetime):
    """Update non-null fields on an existing customer without overwriting existing data."""
    cursor.execute(
        """
        UPDATE [dbo].[Customers]
        SET [BusinessName]    = COALESCE(?, [BusinessName]),
            [Address]         = COALESCE(?, [Address]),
            [TelephoneNumber] = COALESCE(?, [TelephoneNumber]),
            [Email]           = COALESCE(?, [Email]),
            [VatNumber]       = COALESCE(?, [VatNumber]),
            [ModifierUser]    = ?,
            [ModifierDate]    = ?
        WHERE [IdCustomer] = ?
        """,
        (
            data.get("BusinessName") or data.get("business_name"),
            data.get("Address")      or data.get("address"),
            data.get("TelephoneNumber") or data.get("telephone"),
            data.get("Email")        or data.get("email") or data.get("sender_email"),
            data.get("VatNumber")    or data.get("vat_number"),
            modifier_user, now,
            id_customer,
        ),
    )


# ---------------------------------------------------------------------------
# 3. Orders — ONE per ZIP
# ---------------------------------------------------------------------------

def _next_order_sequence(conn: pyodbc.Connection, year: int) -> int:
    cursor = conn.cursor()
    try:
        prefix = f"OFF-HVL-{year}-"
        cursor.execute(
            """
            SELECT MAX(CAST(SUBSTRING([NOrder], LEN(?)+1, 6) AS INT))
            FROM [dbo].[Orders]
            WHERE [NOrder] LIKE ?
            """,
            (prefix, prefix + "%"),
        )
        row = cursor.fetchone()
        current_max = row[0] if (row and row[0] is not None) else 0
        return current_max + 1
    finally:
        cursor.close()


def insert_order(conn: pyodbc.Connection, data: dict, modifier_user: str) -> Tuple[int, str]:
    """
    Insert ONE order for the entire ZIP batch.
    NOrder format: OFF-HVL-YYYY-NNNNNN
    Returns (Id, NOrder).
    """
    now  = datetime.utcnow()
    year = now.year
    note = data.get("order_note") or data.get("Note") or None

    cursor = conn.cursor()
    try:
        seq    = _next_order_sequence(conn, year)
        norder = f"OFF-HVL-{year}-{seq:06d}"

        cursor.execute(
            """
            INSERT INTO [dbo].[Orders]
                ([NOrder],[Note],[CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[Id]
            VALUES (?,?,?,?,?)
            """,
            (norder, note, now, modifier_user, now),
        )
        inserted = cursor.fetchone()
        if not inserted:
            raise Exception(f"Could not retrieve inserted Order ID for NOrder: {norder!r}")
        id_order = inserted[0]
        logger.info(f"Inserted Order id={id_order}, NOrder={norder!r}")
        conn.commit()
        return id_order, norder

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# 4. OrderRows — one per item/file under the same Order
# ---------------------------------------------------------------------------

def insert_order_row(
    conn: pyodbc.Connection,
    id_order: int,
    id_item: int,
    id_customer: int,
    data: dict,
    modifier_user: str,
) -> int:
    """
    Insert a row into [dbo].[OrderRows].
    Fixed: IdOrderType=2, IdOrderStates=7, OrderRow=1
    Returns IdOrderRow.
    """
    now           = datetime.utcnow()
    note          = data.get("order_row_note") or data.get("Note") or None
    delivery_date = data.get("DeliveryDate")   or data.get("delivery_date") or None
    rank          = data.get("Rank")           or data.get("rank")          or None

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO [dbo].[OrderRows]
                ([IdOrderParent],[IdOrderType],[IdOrderStates],[IdCustomer],[IdItem],
                 [OrderRow],[Note],[DeliveryDate],[Rank],[CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[IdOrderRow]
            VALUES (?,2,7,?,?,1,?,?,?,?,?,?)
            """,
            (id_order, id_customer, id_item, note, delivery_date, rank, now, modifier_user, now),
        )
        inserted = cursor.fetchone()
        if not inserted:
            raise Exception("Could not retrieve inserted OrderRow ID")
        id_order_row = inserted[0]
        logger.info(f"Inserted OrderRow id={id_order_row} (Order={id_order}, Item={id_item}, Customer={id_customer})")
        conn.commit()
        return id_order_row

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# 5. OrderValues — ALL params 27–33 always written (default "0" if missing)
# ---------------------------------------------------------------------------

def insert_order_values(
    conn: pyodbc.Connection,
    id_order_row: int,
    data: dict,
    modifier_user: str,
) -> List[int]:
    """
    Insert rows into [dbo].[OrderValues] for ALL 7 parameters (27–33).
    If a value is missing from extraction, writes "0" to ensure the row exists.
    Returns list of inserted IdOrderValues.
    """
    now    = datetime.utcnow()
    ids    = []
    cursor = conn.cursor()

    try:
        for param_id, field_key in ORDER_VALUE_PARAMS.items():
            # Try multiple key variants for flexibility
            value = (
                data.get(field_key)
                or data.get(field_key.replace("_", ""))
                or data.get(field_key.replace("total_painting_surface", "total_surface_area_m2"))
                or data.get(field_key.replace("total_painting_surface", "total_painting_surface_area"))
            )
            # Always write a value — default to "0" if not extracted
            str_value = str(value) if value is not None else "0"

            cursor.execute(
                """
                INSERT INTO [dbo].[OrderValues]
                    ([IdOrderRow],[IdOrderParameter],[Value],[CreationDate],[ModifierUser],[ModifierDate])
                OUTPUT INSERTED.[IdOrderValues]
                VALUES (?,?,?,?,?,?)
                """,
                (id_order_row, param_id, str_value, now, modifier_user, now),
            )
            inserted = cursor.fetchone()
            if inserted:
                ids.append(inserted[0])
                logger.info(f"OrderValue param={param_id} ({field_key}) = {str_value!r}")

        conn.commit()
        return ids

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Convenience: full pipeline (kept for backward compatibility)
# ---------------------------------------------------------------------------

def run_full_pipeline(conn: pyodbc.Connection, extraction: dict, modifier_user: str) -> dict:
    """
    Run the complete pipeline for a single extraction.
    NOTE: For multi-file ZIPs, use routes.py orchestration instead
    (one Order shared across all files).
    """
    id_item     = upsert_item(conn, extraction, modifier_user)
    id_customer = find_or_create_customer(conn, extraction, modifier_user)
    id_order, norder = insert_order(conn, extraction, modifier_user)
    id_order_row = insert_order_row(conn, id_order, id_item, id_customer, extraction, modifier_user)
    ov_ids       = insert_order_values(conn, id_order_row, extraction, modifier_user)

    return {
        "NOrder":        norder,
        "IdItem":        id_item,
        "IdCustomer":    id_customer,
        "IdOrder":       id_order,
        "IdOrderRow":    id_order_row,
        "IdOrderValues": ov_ids,
    }