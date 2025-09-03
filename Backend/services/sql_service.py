# services/sql_service.py
import logging
import pyodbc
from typing import List, Dict, Any, Generator, Union
from utils.credentials_store import load_credentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_sql_connection():
    creds = load_credentials("sql")
    if not creds:
        raise ValueError("SQL Server credentials not found.")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={creds['host']},{creds['port']};"
        f"DATABASE={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def clean_string_value(val, sql_type: str = None):
    if val is None:
        return None
    if isinstance(val, (pyodbc.Date, pyodbc.Time, pyodbc.Timestamp)):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, (bytes, bytearray)):
        return val
    if isinstance(val, str):
        try:
            val.encode("utf-8")
            return val
        except Exception:
            return val.encode("utf-8", errors="replace").decode("utf-8")
    return val

def fetch_schemas() -> List[str]:
    conn = get_sql_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT name FROM sys.schemas WHERE name NOT IN ('sys', 'guest', 'INFORMATION_SCHEMA')
        """)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def fetch_tables(schema: str) -> List[str]:
    conn = get_sql_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT OBJECTNAME FROM [{schema.upper()}].[MAXOBJECT]")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def fetch_table_metadata(schema: str, table: str) -> List[Dict[str, Any]]:
    conn = get_sql_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema.upper(), table.upper()))
        return [{
            "column_name": row[0],
            "data_type": row[1],
            "character_maximum_length": row[2],
            "is_nullable": row[3],
            "numeric_precision": row[4],
            "numeric_scale": row[5]
            } for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def fetch_table_data_generator(schema: str, table: str, count_only=False, batch_size=1000) -> Union[int, Generator[List[Dict[str, Any]], None, None]]:
    conn = get_sql_connection()
    cursor = conn.cursor()
    try:
        if count_only:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, (schema.upper(), table.upper()))
        cols_info = cursor.fetchall()
        columns = [col[0] for col in cols_info]
        type_map = {col[0]: col[1] for col in cols_info}
        offset = 0
        while True:
            query = f"""
                SELECT * FROM [{schema}].[{table}]
                ORDER BY (SELECT NULL)
                OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                break
            batch = [{col: clean_string_value(val, type_map.get(col)) for col, val in zip(columns, row)} for row in rows]
            yield batch
            offset += batch_size
            if len(rows) < batch_size:
                break
    except Exception as e:
        logger.error(f"Error fetching data for {schema}.{table}: {e}")
        yield [{"error": str(e)}]
    finally:
        cursor.close()
        conn.close()

def get_table_row_count(schema: str, table: str) -> int:
    return fetch_table_data_generator(schema, table, count_only=True)

def fetch_sequences(schema: str) -> List[str]:
    conn = get_sql_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT name FROM sys.sequences WHERE SCHEMA_NAME(schema_id) = ?", (schema.upper(),))
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def fetch_indexes(schema: str):
    creds = load_credentials("sql")
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={creds['host']},{creds['port']};DATABASE={creds['database']};UID={creds['username']};PWD={creds['password']}"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT i.name, t.name FROM sys.indexes i
        JOIN sys.tables t ON i.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND i.is_primary_key = 0 AND i.is_unique_constraint = 0
    """, (schema.upper(),))
    indexes = [{"name": row[0], "table": row[1]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return indexes
