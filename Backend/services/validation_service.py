import hashlib
import json
import logging

from utils.credentials_store import get_source_credentials, get_target_credentials
from connections.oracle_connection import get_oracle_connection
from connections.sql_connection import get_sql_connection
from connections.db2_connection import get_db2_connection

def get_quoted_db2_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) == 2:
        schema, tbl = parts
        return f'"{schema}"."{tbl}"'  # Preserve original case
    return f'"{table_name}"'

def fetch_data(conn, table_name):
    try:
        cursor = conn.cursor()

        if '.' not in table_name:
            cursor.execute("""
                SELECT TABLE_SCHEMA 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """, (table_name,))
            result = cursor.fetchone()
            if not result:
                raise RuntimeError(f"Table '{table_name}' not found in any schema")
            schema = result[0]
            table_name = f"{schema}.{table_name}"

        logging.debug(f"[DEBUG] Executing source query: SELECT * FROM {table_name}")
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall()

    except Exception as e:
        raise RuntimeError(f"Error fetching data from source table '{table_name}': {str(e)}")

def get_db2_default_schema(cursor):
    cursor.execute("VALUES CURRENT SCHEMA")
    row = cursor.fetchone()
    return row[0].strip()

# ✅ Returns: rows, resolved_table_name, fallback_used
def fetch_data_db2(conn, table_name):
    cursor = conn.cursor()
    default_schema = get_db2_default_schema(cursor)
    fallback_used = False

    quoted = get_quoted_db2_table_name(table_name)

    try:
        logging.info(f"[DB2] Trying main schema: {quoted}")
        cursor.execute(f"SELECT * FROM {quoted}")
        return cursor.fetchall(), quoted, fallback_used

    except Exception as e1:
        try:
            tbl_only = table_name.split(".")[-1]
            fallback = f'"{default_schema}"."{tbl_only}"'
            logging.warning(f"[⚠️ FALLBACK] Using default schema '{default_schema}' for '{tbl_only}'")
            cursor.execute(f"SELECT * FROM {fallback}")
            fallback_used = True
            return cursor.fetchall(), fallback, fallback_used

        except Exception as e2:
            raise RuntimeError(f"Error fetching data from DB2 table '{table_name}': {str(e2)}")

def hash_data(rows):
    sanitized_rows = [
        tuple("" if val is None else val for val in row)
        for row in rows
    ]
    rows_sorted = sorted(sanitized_rows)
    json_rows = json.dumps(rows_sorted, default=str)
    return hashlib.md5(json_rows.encode()).hexdigest()

def get_source_conn(source_type):
    creds = get_source_credentials(source_type)
    logging.debug(f"[DEBUG] Connecting to source {source_type}")
    if source_type.lower() == "oracle":
        return get_oracle_connection(creds)
    elif source_type.lower() == "sql":
        return get_sql_connection(creds)
    else:
        raise ValueError("Invalid source type. Must be 'oracle' or 'sql'.")

def validate_table(table_name: str, source_type: str):
    source_conn = get_source_conn(source_type)
    target_creds = get_target_credentials()
    target_conn = get_db2_connection(target_creds)

    try:
        source_data = fetch_data(source_conn, table_name)
        target_data, resolved_table_name, fallback_used = fetch_data_db2(target_conn, table_name)

        source_hash = hash_data(source_data)
        target_hash = hash_data(target_data)

        row_count_source = len(source_data)
        row_count_target = len(target_data)

        # ✅ Match only if both row count and hash match
        match = (row_count_source == row_count_target) and (source_hash == target_hash)

        # ⚠️ If fallback schema used but data matches, still mark mismatch if row count is different
        if fallback_used and match is False:
            logging.warning(f"[INFO] Table fallback schema used and data mismatch found — keeping mismatch for {table_name}")

        return {
            "table": table_name,
            "match": match,
            "source_hash": source_hash,
            "target_hash": target_hash,
            "row_count_source": row_count_source,
            "row_count_target": row_count_target,
            "db2_table_used": resolved_table_name,
            "fallback_used": fallback_used
        }

    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()


def validate_multiple_tables(table_names, source_type):
    return [validate_table(name, source_type) for name in table_names]

def validate_schema(source_type, schema):
    logging.info(f"🧪 Validating entire schema: {schema} from source: {source_type}")
    conn = get_source_conn(source_type)
    cursor = conn.cursor()

    if source_type.lower() == "oracle":
        cursor.execute(f"SELECT table_name FROM all_tables WHERE owner = UPPER('{schema}')")
    elif source_type.lower() == "sql":
        cursor.execute(f"""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE'
        """)
    else:
        raise ValueError("Unsupported source type")

    tables = [f"{schema}.{row[0]}" for row in cursor.fetchall()]
    return validate_multiple_tables(tables, source_type)
