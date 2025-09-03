
# === services/embedded_sql_service.py===

import os
import oracledb
# oracledb.init_oracle_client(lib_dir=None)
from typing import List, Dict, Any, Set, Tuple

from connections.oracle_connection import get_oracle_connection
from utils.config_loader import load_yaml_config

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "embedded_sql.yml")
CONFIG = load_yaml_config(CONFIG_PATH)

# New dict keyed by (table, column) with value as identifier_column_name
ALLOWED_TABLE_COLUMNS: Dict[Tuple[str, str], str] = {
    (entry["table"].upper(), entry["column"].upper()): entry.get("identifier_column_name", "?")
    for entry in CONFIG.get("allowed_table_columns", [])
}


def load_allowed_table_columns() -> Dict[Tuple[str, str], str]:
    """Return allowed table-column pairs with their identifier column names"""
    return ALLOWED_TABLE_COLUMNS


def fetch_existing_table_columns() -> List[Dict[str, str]]:
    with get_oracle_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
        schema_row = cursor.fetchone()
        if not schema_row or not schema_row[0]:
            raise RuntimeError("Unable to determine CURRENT_SCHEMA from Oracle.")
        schema = str(schema_row[0]).strip().upper()

        allowed_pairs_with_id = load_allowed_table_columns()
        allowed_tables = {tbl for (tbl, _) in allowed_pairs_with_id.keys()}
        if not allowed_tables:
            return []

        in_clause = "(" + ",".join(f"'{tbl}'" for tbl in sorted(allowed_tables)) + ")"

        query = f"""
            SELECT table_name, column_name
            FROM all_tab_columns
            WHERE owner = :schema
              AND table_name IN {in_clause}
        """
        cursor.execute(query, {"schema": schema})
        existing_pairs = {(row[0].upper(), row[1].upper()) for row in cursor.fetchall()}

        # Return including identifier_column_name for UI
        return [
            {
                "table": tbl,
                "column": col,
                "identifier_column_name": allowed_pairs_with_id.get((tbl, col), "?"),
            }
            for (tbl, col) in sorted(allowed_pairs_with_id.keys())
            if (tbl, col) in existing_pairs
        ]


def fetch_matching_rows(table: str, function_name: str) -> List[Dict[str, Any]] | Dict[str, str]:
    table = (table or "").strip().upper()
    function_name = (function_name or "").strip()

    if not table:
        return {"error": "Table is required."}
    if not function_name:
        return {"error": "function_name is required."}

    with get_oracle_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
        schema_row = cursor.fetchone()
        if not schema_row or not schema_row[0]:
            return {"error": "Unable to determine CURRENT_SCHEMA from Oracle."}
        schema = str(schema_row[0]).strip().upper()

        allowed_pairs_with_id = load_allowed_table_columns()
        valid_columns = [col for (tbl, col) in allowed_pairs_with_id.keys() if tbl == table]
        if not valid_columns:
            return {"error": f"Table '{table}' is not allowed or has no allowed columns."}

        result: List[Dict[str, Any]] = []
        for col in sorted(valid_columns):
            identifier_col = allowed_pairs_with_id.get((table, col), "?")

            # Build SELECT list
            select_columns = f't."{col}"'
            # Add identifier column if known and not '?', else handle accordingly
            if identifier_col and identifier_col != "?" and identifier_col.upper() != col.upper():
                select_columns += f', t."{identifier_col}"'

            try:
                query = f'''
                    SELECT :table_name AS table_name,
                           :column_name AS column_name,
                           {select_columns}
                    FROM "{schema}"."{table}" t
                    WHERE UPPER(t."{col}") LIKE UPPER(:pattern)
                    FETCH FIRST 100 ROWS ONLY
                '''
                cursor.execute(
                    query,
                    {
                        "pattern": f"%{function_name}%",
                        "table_name": table,
                        "column_name": col,
                    },
                )
                columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row))
                    # If identifier column missing in row_dict (because '?'), add '?'
                    if identifier_col == "?" or identifier_col.upper() == col.upper():
                        row_dict["IDENTIFIER_VALUE"] = "?"
                    else:
                        # identifier_col value is included automatically as per select_columns
                        # Need to find correct key matching identifier column
                        # The select adds the identifier column as same name, so fetch from row based on position
                        # Safe to get by index in cursor.description
                        # For neatness, rename identifier column to IDENTIFIER_VALUE for frontend
                        idx = columns.index(identifier_col)
                        row_dict["IDENTIFIER_VALUE"] = row[idx]
                    result.append(row_dict)
            except Exception as e:
                result.append({"error": str(e), "column": col})

        return result




def fetch_all_schemas() -> List[str]:
    """Fetch all Oracle schemas"""
    with get_oracle_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT USERNAME FROM ALL_USERS")
        return [row[0] for row in cursor.fetchall()]

