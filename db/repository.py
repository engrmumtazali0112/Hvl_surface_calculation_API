"""
db/repository.py
----------------
All database operations for:
  - Items          (upsert by ItemCode)
  - Customers      (upsert by CustomerIdentityCode)
  - Orders         (insert with sequential NOrder: OFF-HVL-YYYY-NNNNNN)
  - OrderRows      (insert)
  - OrderValues    (insert per parameter)

Every function receives an open pyodbc.Connection.
All writes are committed inside each function.
"""

import pyodbc
from datetime import datetime, date
from typing import Optional
import logging

logger = logging.getLogger(__name__)

MODIFIER_USER = "HVL-API"

# ── OrderValues parameter IDs ─────────────────────────────────────────────────
ORDER_VALUE_PARAMS = {
    27: "ral_color",
    28: "finishing_type",
    29: "batch_size",
    30: "pitch",
    31: "presence_of_protections",
    32: "type_of_protections",
    33: "total_painting_surface_area",
}


# ═════════════════════════════════════════════════════════════════════════════
# ITEMS
# ═════════════════════════════════════════════════════════════════════════════

def upsert_item(conn: pyodbc.Connection, item: dict) -> int:
    """
    Upsert into [dbo].[Items] keyed on ItemCode.
    Returns IdItem.

    Expected keys in item dict (all optional except item_code):
        item_code, item_description, revision
    """
    cursor = conn.cursor()
    now = datetime.utcnow()
    item_code = item.get("item_code") or item.get("ItemCode")

    if not item_code:
        raise ValueError("item_code is required for upsert")

    # Check existence
    cursor.execute(
        "SELECT [IdItem] FROM [dbo].[Items] WHERE [ItemCode] = ?",
        item_code,
    )
    row = cursor.fetchone()

    if row:
        # UPDATE
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
            item.get("item_description"),
            item.get("revision"),
            MODIFIER_USER,
            now,
            id_item,
        )
        logger.info("Updated item id=%s code=%s", id_item, item_code)
    else:
        # INSERT
        cursor.execute(
            """
            INSERT INTO [dbo].[Items]
                ([ItemCode],[ItemDescription],[Revision],
                 [CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[IdItem]
            VALUES (?,?,?,?,?,?)
            """,
            item_code,
            item.get("item_description"),
            item.get("revision"),
            now,
            MODIFIER_USER,
            now,
        )
        id_item = cursor.fetchone()[0]
        logger.info("Inserted item id=%s code=%s", id_item, item_code)

    conn.commit()
    return id_item


# ═════════════════════════════════════════════════════════════════════════════
# CUSTOMERS
# ═════════════════════════════════════════════════════════════════════════════

def upsert_customer(conn: pyodbc.Connection, customer: dict) -> int:
    """
    Upsert into [dbo].[Customers] keyed on CustomerIdentityCode.
    Returns IdCustomer.

    Expected keys (all optional except identity_code):
        identity_code, business_name, address, cap, city, province,
        country, telephone_number, fax, vat_number, fiscal_code,
        note, email
    """
    cursor = conn.cursor()
    now = datetime.utcnow()
    identity_code = (
        customer.get("identity_code")
        or customer.get("CustomerIdentityCode")
    )

    if not identity_code:
        raise ValueError("identity_code is required for customer upsert")

    cursor.execute(
        "SELECT [IdCustomer] FROM [dbo].[Customers] "
        "WHERE [CustomerIdentityCode] = ?",
        identity_code,
    )
    row = cursor.fetchone()

    if row:
        id_customer = row[0]
        cursor.execute(
            """
            UPDATE [dbo].[Customers]
            SET [BusinessName]    = COALESCE(?, [BusinessName]),
                [Address]         = COALESCE(?, [Address]),
                [Cap]             = COALESCE(?, [Cap]),
                [City]            = COALESCE(?, [City]),
                [Province]        = COALESCE(?, [Province]),
                [Country]         = COALESCE(?, [Country]),
                [TelephoneNumber] = COALESCE(?, [TelephoneNumber]),
                [Fax]             = COALESCE(?, [Fax]),
                [VatNumber]       = COALESCE(?, [VatNumber]),
                [FiscalCode]      = COALESCE(?, [FiscalCode]),
                [Note]            = COALESCE(?, [Note]),
                [Email]           = COALESCE(?, [Email]),
                [ModifierUser]    = ?,
                [ModifierDate]    = ?
            WHERE [IdCustomer] = ?
            """,
            customer.get("business_name"),
            customer.get("address"),
            customer.get("cap"),
            customer.get("city"),
            customer.get("province"),
            customer.get("country"),
            customer.get("telephone_number"),
            customer.get("fax"),
            customer.get("vat_number"),
            customer.get("fiscal_code"),
            customer.get("note"),
            customer.get("email"),
            MODIFIER_USER,
            now,
            id_customer,
        )
        logger.info("Updated customer id=%s code=%s", id_customer, identity_code)
    else:
        cursor.execute(
            """
            INSERT INTO [dbo].[Customers]
                ([CustomerIdentityCode],[BusinessName],[Address],[Cap],
                 [City],[Province],[Country],[TelephoneNumber],[Fax],
                 [VatNumber],[FiscalCode],[Note],[Email],
                 [CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[IdCustomer]
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            identity_code,
            customer.get("business_name"),
            customer.get("address"),
            customer.get("cap"),
            customer.get("city"),
            customer.get("province"),
            customer.get("country"),
            customer.get("telephone_number"),
            customer.get("fax"),
            customer.get("vat_number"),
            customer.get("fiscal_code"),
            customer.get("note"),
            customer.get("email"),
            now,
            MODIFIER_USER,
            now,
        )
        id_customer = cursor.fetchone()[0]
        logger.info("Inserted customer id=%s code=%s", id_customer, identity_code)

    conn.commit()
    return id_customer


# ═════════════════════════════════════════════════════════════════════════════
# ORDERS  –  sequential NOrder: OFF-HVL-YYYY-NNNNNN
# ═════════════════════════════════════════════════════════════════════════════

def _next_norder(cursor: pyodbc.Cursor) -> str:
    """
    Generate the next sequential NOrder for the current year.
    Format: OFF-HVL-YYYY-NNNNNN (6-digit zero-padded sequence).
    Sequence resets per year.
    """
    year = datetime.utcnow().year
    prefix = f"OFF-HVL-{year}-"

    cursor.execute(
        """
        SELECT TOP 1 [NOrder]
        FROM [dbo].[Orders]
        WHERE [NOrder] LIKE ?
        ORDER BY [NOrder] DESC
        """,
        f"{prefix}%",
    )
    row = cursor.fetchone()

    if row:
        last_seq = int(row[0].split("-")[-1])
        next_seq = last_seq + 1
    else:
        next_seq = 1

    return f"{prefix}{next_seq:06d}"


def insert_order(conn: pyodbc.Connection, note: Optional[str] = None) -> tuple[int, str]:
    """
    Insert a new order with auto-generated NOrder.
    Returns (Id, NOrder).
    """
    cursor = conn.cursor()
    now = datetime.utcnow()
    norder = _next_norder(cursor)

    cursor.execute(
        """
        INSERT INTO [dbo].[Orders]
            ([NOrder],[Note],[CreationDate],[ModifierUser],[ModifierDate])
        OUTPUT INSERTED.[Id]
        VALUES (?,?,?,?,?)
        """,
        norder,
        note,
        now,
        MODIFIER_USER,
        now,
    )
    id_order = cursor.fetchone()[0]
    conn.commit()
    logger.info("Inserted order id=%s NOrder=%s", id_order, norder)
    return id_order, norder


# ═════════════════════════════════════════════════════════════════════════════
# ORDER ROWS
# ═════════════════════════════════════════════════════════════════════════════

def insert_order_row(
    conn: pyodbc.Connection,
    id_order: int,
    id_customer: int,
    id_item: int,
    delivery_date: Optional[date] = None,
    note: Optional[str] = None,
    rank: Optional[int] = None,
) -> int:
    """
    Insert into [dbo].[OrderRows].
    Fixed values: IdOrderType=2, IdOrderStates=7, OrderRow=1
    Returns IdOrderRow.
    """
    cursor = conn.cursor()
    now = datetime.utcnow()

    cursor.execute(
        """
        INSERT INTO [dbo].[OrderRows]
            ([IdOrderParent],[IdOrderType],[IdOrderStates],
             [IdCustomer],[IdItem],[OrderRow],
             [Note],[DeliveryDate],[Rank],
             [CreationDate],[ModifierUser],[ModifierDate])
        OUTPUT INSERTED.[IdOrderRow]
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        id_order,
        2,           # IdOrderType  – fixed
        7,           # IdOrderStates – fixed
        id_customer,
        id_item,
        1,           # OrderRow – fixed
        note,
        delivery_date,
        rank,
        now,
        MODIFIER_USER,
        now,
    )
    id_order_row = cursor.fetchone()[0]
    conn.commit()
    logger.info(
        "Inserted order row id=%s (order=%s, item=%s, customer=%s)",
        id_order_row, id_order, id_item, id_customer,
    )
    return id_order_row


# ═════════════════════════════════════════════════════════════════════════════
# ORDER VALUES  (parameters 27-33)
# ═════════════════════════════════════════════════════════════════════════════

def insert_order_values(
    conn: pyodbc.Connection,
    id_order_row: int,
    values: dict,
) -> list[int]:
    """
    Insert into [dbo].[OrderValues] for each of the 7 parameters (27-33).
    `values` dict keys map to ORDER_VALUE_PARAMS field names.
    Skips parameters where value is None.
    Returns list of inserted IdOrderValues.
    """
    cursor = conn.cursor()
    now = datetime.utcnow()
    inserted_ids = []

    for param_id, field_name in ORDER_VALUE_PARAMS.items():
        value = values.get(field_name)
        if value is None:
            logger.debug("Skipping param %s (%s) – no value", param_id, field_name)
            continue

        cursor.execute(
            """
            INSERT INTO [dbo].[OrderValues]
                ([IdOrderRow],[IdOrderParameter],[Value],
                 [CreationDate],[ModifierUser],[ModifierDate])
            OUTPUT INSERTED.[IdOrderValues]
            VALUES (?,?,?,?,?,?)
            """,
            id_order_row,
            param_id,
            str(value),
            now,
            MODIFIER_USER,
            now,
        )
        id_val = cursor.fetchone()[0]
        inserted_ids.append(id_val)
        logger.debug("Inserted order value param=%s value=%s", param_id, value)

    conn.commit()
    return inserted_ids