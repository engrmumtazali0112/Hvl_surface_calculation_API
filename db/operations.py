"""
db/operations.py — All database operations for the HVL Surface pipeline.

Pipeline (called once per ZIP):
  1.  find_or_create_customer   — match by name → identity_code → email domain → insert
  2.  insert_order              — ONE Order per ZIP, sequential NOrder OFF-HVL-YYYY-NNNNNN
  3.  upsert_item               — per extracted item (update if exists by ItemCode, else insert)
  4.  insert_order_row          — per item, linked to the single Order + Customer
  5.  insert_order_values       — params 27–34 always written; "No" when value is absent
  6.  insert_process_and_phases — ProcessesList + PhasesList per OrderRow (best-effort)
"""
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pyodbc

logger = logging.getLogger("db.operations")

# ---------------------------------------------------------------------------
# OrderValues parameter map  (params 27–34)
# ---------------------------------------------------------------------------
ORDER_VALUE_PARAMS: Dict[int, str] = {
    27: "ral_color",
    28: "finishing_type",
    29: "batch_size",
    30: "pitch",
    31: "presence_of_protections",
    32: "type_of_protections",
    33: "total_painting_surface",
    34: "production_line",
}

# Keywords used to detect painting-relevant phases in PhasesCompany
PAINTING_PHASE_KEYWORDS = [
    "verniciatura", "paint", "coating", "powder", "primer",
    "masking", "mascheratura", "finitura", "surface", "lacca",
    "vernicia", "trattamento", "treatment",
]


# ===========================================================================
# 1. Customer — find or create (never duplicate)
# ===========================================================================

def find_or_create_customer(
    conn: pyodbc.Connection, data: dict, modifier_user: str
) -> int:
    """
    Search order: BusinessName exact → BusinessName partial → CustomerIdentityCode
                  → email domain → insert new.
    Never inserts a duplicate.
    """
    business_name = (
        data.get("BusinessName") or data.get("business_name")
        or data.get("sender_name") or data.get("from_name")
    )
    identity_code = (
        data.get("CustomerIdentityCode") or data.get("customer_identity_code")
        or data.get("customer_code")
    )
    email_addr = data.get("Email") or data.get("email") or data.get("sender_email")

    now    = datetime.utcnow()
    cursor = conn.cursor()

    try:
        # ── Step 1: exact name match ─────────────────────────────────────────
        if business_name:
            cursor.execute(
                """
                SELECT TOP 1 [IdCustomer], [BusinessName]
                FROM [dbo].[Customers]
                WHERE LOWER(LTRIM(RTRIM([BusinessName]))) = LOWER(LTRIM(RTRIM(?)))
                """,
                (business_name,),
            )
            row = cursor.fetchone()
            if row:
                id_c = row[0]
                logger.info(f"Customer found by name: id={id_c}, name={row[1]!r}")
                _update_customer_fields(cursor, id_c, data, modifier_user, now)
                conn.commit()
                return id_c

        # ── Step 2: partial name match (first significant word) ──────────────
        if business_name:
            first_word = business_name.split()[0] if business_name.split() else ""
            if len(first_word) > 2:  # ignore very short words
                cursor.execute(
                    """
                    SELECT TOP 1 [IdCustomer], [BusinessName]
                    FROM [dbo].[Customers]
                    WHERE LOWER([BusinessName]) LIKE LOWER(?)
                    """,
                    (f"%{first_word}%",),
                )
                row = cursor.fetchone()
                if row:
                    id_c = row[0]
                    logger.info(
                        f"Customer found by partial name: id={id_c}, name={row[1]!r}"
                    )
                    _update_customer_fields(cursor, id_c, data, modifier_user, now)
                    conn.commit()
                    return id_c

        # ── Step 3: CustomerIdentityCode ─────────────────────────────────────
        if identity_code:
            cursor.execute(
                "SELECT [IdCustomer] FROM [dbo].[Customers] WHERE [CustomerIdentityCode] = ?",
                (identity_code,),
            )
            row = cursor.fetchone()
            if row:
                id_c = row[0]
                logger.info(f"Customer found by identity code: id={id_c}")
                _update_customer_fields(cursor, id_c, data, modifier_user, now)
                conn.commit()
                return id_c

        # ── Step 4: email domain ─────────────────────────────────────────────
        if email_addr and "@" in email_addr:
            domain = email_addr.split("@")[-1]
            cursor.execute(
                "SELECT TOP 1 [IdCustomer] FROM [dbo].[Customers] WHERE [Email] LIKE ?",
                (f"%@{domain}",),
            )
            row = cursor.fetchone()
            if row:
                id_c = row[0]
                logger.info(f"Customer found by email domain: id={id_c}")
                _update_customer_fields(cursor, id_c, data, modifier_user, now)
                conn.commit()
                return id_c

        # ── Step 5: insert new customer ──────────────────────────────────────
        if not identity_code:
            if email_addr and "@" in email_addr:
                domain_part = email_addr.split("@")[-1].split(".")[0].upper()
                identity_code = f"CUST-{domain_part}"
            elif business_name:
                import hashlib
                identity_code = "CUST-" + hashlib.md5(
                    business_name.encode()
                ).hexdigest()[:8].upper()
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
                data.get("Address")         or data.get("address"),
                data.get("Cap")             or data.get("cap"),
                data.get("City")            or data.get("city"),
                data.get("Province")        or data.get("province"),
                data.get("Country")         or data.get("country"),
                data.get("TelephoneNumber") or data.get("telephone"),
                data.get("Fax")             or data.get("fax"),
                data.get("VatNumber")       or data.get("vat_number"),
                data.get("FiscalCode")      or data.get("fiscal_code"),
                data.get("Note")            or data.get("note"),
                email_addr,
                now, modifier_user, now,
            ),
        )
        inserted = cursor.fetchone()
        if not inserted:
            raise Exception(
                f"Could not retrieve inserted Customer ID for code: {identity_code!r}"
            )
        id_c = inserted[0]
        logger.info(
            f"Inserted Customer id={id_c}, code={identity_code!r}, "
            f"name={business_name!r}"
        )
        conn.commit()
        return id_c

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _update_customer_fields(
    cursor, id_customer: int, data: dict, modifier_user: str, now: datetime
) -> None:
    """Update non-null fields on an existing customer row."""
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
            data.get("BusinessName")    or data.get("business_name"),
            data.get("Address")         or data.get("address"),
            data.get("TelephoneNumber") or data.get("telephone"),
            data.get("Email")           or data.get("email"),
            data.get("VatNumber")       or data.get("vat_number"),
            modifier_user, now, id_customer,
        ),
    )


# ===========================================================================
# 2. Orders — ONE per ZIP, sequential NOrder
# ===========================================================================

def _next_order_sequence(conn: pyodbc.Connection, year: int) -> int:
    prefix = f"OFF-HVL-{year}-"
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT MAX(CAST(SUBSTRING([NOrder], LEN(?)+1, 6) AS INT))
            FROM [dbo].[Orders]
            WHERE [NOrder] LIKE ?
            """,
            (prefix, prefix + "%"),
        )
        row         = cursor.fetchone()
        current_max = row[0] if (row and row[0] is not None) else 0
        return current_max + 1
    finally:
        cursor.close()


def insert_order(
    conn: pyodbc.Connection, data: dict, modifier_user: str
) -> Tuple[int, str]:
    """Insert ONE Order for the entire ZIP. Returns (id_order, norder)."""
    now    = datetime.utcnow()
    year   = now.year
    note   = data.get("order_note") or data.get("Note") or None
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


# ===========================================================================
# 3. Items — upsert by ItemCode
# ===========================================================================

def upsert_item(
    conn: pyodbc.Connection, data: dict, modifier_user: str
) -> int:
    """Update item if ItemCode exists, otherwise insert. Returns IdItem."""
    code = data.get("ItemCode") or data.get("item_code")

    if not code:
        # Derive a code from description or filename
        desc = (
            data.get("ItemDescription") or data.get("item_description")
            or data.get("source_file") or ""
        )
        if desc:
            code = re.sub(r"[^A-Za-z0-9\-_.]", "_", desc.strip())[:50]
            logger.warning(f"ItemCode was None — derived: {code!r}")
        else:
            raise ValueError("Cannot upsert item: ItemCode is None and no fallback available.")

    code        = str(code)[:100]
    description = (
        (data.get("ItemDescription") or data.get("item_description") or "")[:500] or None
    )
    revision    = (data.get("Revision") or data.get("revision") or "")[:50] or None
    now         = datetime.utcnow()
    cursor      = conn.cursor()

    try:
        cursor.execute(
            "SELECT [IdItem] FROM [dbo].[Items] WHERE [ItemCode] = ?",
            (code,),
        )
        row = cursor.fetchone()

        if row:
            id_item = row[0]
            cursor.execute(
                """
                UPDATE [dbo].[Items]
                SET [ItemDescription] = COALESCE(?, [ItemDescription]),
                    [Revision]        = COALESCE(?, [Revision]),
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
                    ([ItemCode],[ItemDescription],[Revision],
                     [CreationDate],[ModifierUser],[ModifierDate])
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


# ===========================================================================
# 4. OrderRows — one per item, all linked to the SAME order
# ===========================================================================

def insert_order_row(
    conn: pyodbc.Connection,
    id_order: int,
    id_item: int,
    id_customer: int,
    data: dict,
    modifier_user: str,
) -> int:
    """Insert one OrderRow. Returns IdOrderRow."""
    now           = datetime.utcnow()
    note          = data.get("order_row_note") or data.get("Note") or None
    delivery_date = data.get("DeliveryDate")   or data.get("delivery_date") or None
    rank          = data.get("Rank")           or data.get("rank")          or None
    cursor        = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO [dbo].[OrderRows]
                ([IdOrderParent],[IdOrderType],[IdOrderStates],[IdCustomer],[IdItem],
                 [OrderRow],[Note],[DeliveryDate],[Rank],[CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[IdOrderRow]
            VALUES (?,2,7,?,?,1,?,?,?,?,?,?)
            """,
            (
                id_order, id_customer, id_item,
                note, delivery_date, rank,
                now, modifier_user, now,
            ),
        )
        inserted = cursor.fetchone()
        if not inserted:
            raise Exception("Could not retrieve inserted OrderRow ID")
        id_order_row = inserted[0]
        logger.info(
            f"Inserted OrderRow id={id_order_row} "
            f"(Order={id_order}, Item={id_item}, Customer={id_customer})"
        )
        conn.commit()
        return id_order_row

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# ===========================================================================
# 5. OrderValues — params 27–34, always written, "No" when value absent
# ===========================================================================

def insert_order_values(
    conn: pyodbc.Connection,
    id_order_row: int,
    data: dict,
    modifier_user: str,
) -> List[int]:
    """
    Write params 27–34 into [dbo].[OrderValues].
    Rule: write "No" for any parameter that has no value.
    Never write "0" for categorical parameters.
    """
    now    = datetime.utcnow()
    ids    = []
    cursor = conn.cursor()

    try:
        for param_id, field_key in ORDER_VALUE_PARAMS.items():
            raw = data.get(field_key)

            # Normalise to string value or "No"
            if raw is None or str(raw).strip().lower() in ("", "none", "null"):
                str_val = "No"
            else:
                str_val = str(raw).strip()
                if str_val == "0":
                    str_val = "No"  # legacy guard — "0" is not a valid categorical

            cursor.execute(
                """
                INSERT INTO [dbo].[OrderValues]
                    ([IdOrderRow],[IdOrderParameter],[Value],
                     [CreationDate],[ModifierUser],[ModifierDate])
                OUTPUT INSERTED.[IdOrderValues]
                VALUES (?,?,?,?,?,?)
                """,
                (id_order_row, param_id, str_val, now, modifier_user, now),
            )
            inserted = cursor.fetchone()
            if inserted:
                ids.append(inserted[0])
                logger.info(f"OrderValue param={param_id} ({field_key}) = {str_val!r}")

        conn.commit()
        return ids

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


# ===========================================================================
# 6. ProcessesList + PhasesList — best-effort (table may not exist)
# ===========================================================================

def _inspect_phases_company(conn: pyodbc.Connection) -> Dict[str, str]:
    """
    Return column-name mapping for [dbo].[PhasesCompany]:
        { 'pk': <col>, 'code': <col>, 'desc': <col> }
    Returns {} if table is absent or unreadable.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'PhasesCompany'"
        )
        cols = {r[0].lower(): r[0] for r in cursor.fetchall()}
    except Exception as e:
        logger.warning(f"Cannot inspect PhasesCompany: {e}")
        return {}
    finally:
        cursor.close()

    if not cols:
        return {}

    schema: Dict[str, str] = {}
    for candidate in ("idphasecompany", "idphase", "id"):
        if candidate in cols:
            schema["pk"] = cols[candidate]
            break
    for candidate in ("phasecode", "code", "codice"):
        if candidate in cols:
            schema["code"] = cols[candidate]
            break
    for candidate in ("phasedescription", "phasename", "description", "descrizione", "name"):
        if candidate in cols:
            schema["desc"] = cols[candidate]
            break

    return schema


def _get_painting_phases(conn: pyodbc.Connection) -> List[dict]:
    """
    Return PhasesCompany rows whose text matches painting keywords.
    Falls back to the first available phase if none match.
    Returns [] if table is missing or empty.
    """
    schema = _inspect_phases_company(conn)
    if not schema.get("pk"):
        logger.warning("PhasesCompany PK not found — PhasesList will be skipped")
        return []

    pk   = schema["pk"]
    code = schema.get("code", pk)
    desc = schema.get("desc", code)

    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT [{pk}], [{code}], [{desc}] FROM [dbo].[PhasesCompany]"
        )
        rows = cursor.fetchall()
    except Exception as e:
        logger.warning(f"PhasesCompany query failed: {e}")
        return []
    finally:
        cursor.close()

    if not rows:
        return []

    matched = [
        {
            "IdPhaseCompany":   r[0],
            "PhaseCode":        r[1],
            "PhaseDescription": r[2],
        }
        for r in rows
        if any(kw in " ".join(str(v).lower() for v in r if v) for kw in PAINTING_PHASE_KEYWORDS)
    ]

    if not matched:
        logger.warning(
            "No painting-specific phases in PhasesCompany — using first available"
        )
        matched = [{"IdPhaseCompany": rows[0][0], "PhaseCode": rows[0][1], "PhaseDescription": rows[0][2]}]

    logger.info(f"PhasesCompany: {len(matched)} painting phase(s) selected")
    return matched


def insert_process_and_phases(
    conn: pyodbc.Connection,
    id_order_row: int,
    id_item: int,
    data: dict,
    modifier_user: str,
) -> dict:
    """
    Insert ONE ProcessesList row + one PhasesList row per painting phase.
    If ProcessesList table doesn't exist, logs a warning and returns nulls.
    Returns { id_process_list: int|None, id_phase_list: [int, ...] }
    """
    now       = datetime.utcnow()
    item_code = data.get("ItemCode") or data.get("item_code") or ""
    cursor    = conn.cursor()

    try:
        # ── ProcessesList ─────────────────────────────────────────────────────
        try:
            cursor.execute(
                """
                INSERT INTO [dbo].[ProcessesList]
                    ([IdOrderRow],[IdItem],[ProcessCode],
                     [ModifierUser],[ModifierDate],[CreationDate])
                OUTPUT INSERTED.[IdProcessList]
                VALUES (?,?,?,?,?,?)
                """,
                (id_order_row, id_item, item_code, modifier_user, now, now),
            )
            inserted = cursor.fetchone()
            if not inserted:
                raise Exception("OUTPUT clause returned no row")
            id_process_list = inserted[0]
            conn.commit()
            logger.info(
                f"Inserted ProcessesList id={id_process_list} "
                f"for OrderRow={id_order_row}"
            )
        except pyodbc.Error as e:
            conn.rollback()
            logger.warning(
                f"ProcessesList insert failed (table may not exist or columns differ): {e}"
            )
            return {"id_process_list": None, "id_phase_list": []}

        # ── PhasesList ────────────────────────────────────────────────────────
        phases    = _get_painting_phases(conn)
        phase_ids = []

        for seq, phase in enumerate(phases, start=1):
            id_phase_company = phase.get("IdPhaseCompany")
            if id_phase_company is None:
                continue
            try:
                cursor.execute(
                    """
                    INSERT INTO [dbo].[PhasesList]
                        ([IdProcessList],[IdPhaseCompany],
                         [ModifierUser],[ModifierDate],[CreationDate],[Sequences])
                    OUTPUT INSERTED.[IdPhaseList]
                    VALUES (?,?,?,?,?,?)
                    """,
                    (id_process_list, id_phase_company, modifier_user, now, now, seq),
                )
                row = cursor.fetchone()
                if row:
                    phase_ids.append(row[0])
                    logger.info(
                        f"Inserted PhasesList id={row[0]} "
                        f"(Process={id_process_list}, "
                        f"PhaseCompany={id_phase_company}, seq={seq})"
                    )
                conn.commit()
            except pyodbc.Error as e:
                conn.rollback()
                logger.warning(
                    f"PhasesList insert failed for phase {id_phase_company}: {e}"
                )

        return {"id_process_list": id_process_list, "id_phase_list": phase_ids}

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
