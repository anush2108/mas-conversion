# services/oracle_service.py
import logging
import oracledb
from typing import List, Dict, Any, Generator, Union
import threading
from utils.credentials_store import load_credentials
from utils.ddl_writer import save_ddl


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_oracle_connection():
    creds = load_credentials("oracle")
    if not creds:
        raise ValueError("Oracle credentials not found.")
    CONNECT_TIMEOUT = 999999  # seconds
    RECV_TIMEOUT = 999999     # seconds
    if creds.get("sid"):
        dsn = (
            f"(DESCRIPTION="
            f"(ADDRESS=(PROTOCOL=TCP)(HOST={creds['host']})(PORT={creds['port']})(CONNECT_TIMEOUT={CONNECT_TIMEOUT})(RECV_TIMEOUT={RECV_TIMEOUT}))"
            f"(CONNECT_DATA=(SID={creds['sid']}))"
            f")"
        )
    elif creds.get("service_name"):
        dsn = (
            f"(DESCRIPTION="
            f"(ADDRESS=(PROTOCOL=TCP)(HOST={creds['host']})(PORT={creds['port']})(CONNECT_TIMEOUT={CONNECT_TIMEOUT})(RECV_TIMEOUT={RECV_TIMEOUT}))"
            f"(CONNECT_DATA=(SERVICE_NAME={creds['service_name']}))"
            f")"
        )
    else:
        dsn = oracledb.makedsn(creds["host"], int(creds["port"]), sid=creds.get("sid"))
    # Consider adding connection retry logic here if needed
    return oracledb.connect(user=creds["username"], password=creds["password"], dsn=dsn)


def clean_string_value(val, oracle_type: str = None):
    if val is None:
        return None
    if isinstance(val, (oracledb.Date, oracledb.Timestamp)):
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


def quote_identifier(identifier: str) -> str:
    return f'"{identifier.upper()}"' if identifier else '""'


def fetch_schemas() -> List[str]:
    conn = get_oracle_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT OWNER FROM ALL_TABLES
            WHERE OWNER NOT IN ('SYS','SYSTEM')
        """)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def fetch_tables(schema: str) -> List[str]:
    conn = get_oracle_connection()
    cursor = conn.cursor()
    try:
        # Inject schema directly with quoting to avoid bind variable error
        sql = f"""
            SELECT OBJECTNAME
            FROM {quote_identifier(schema)}.MAXOBJECT
            WHERE UPPER(OBJECTNAME) IN (
                SELECT UPPER(TABLE_NAME)
                FROM ALL_TABLES
                WHERE OWNER = '{schema.upper()}'
            )
        """
        cursor.execute(sql)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


_metadata_cache = {}
_cache_lock = threading.Lock()


def fetch_table_metadata(schema: str, table: str) -> List[Dict[str, Any]]:
    schema_up = schema.upper()
    table_up = table.upper()
    with _cache_lock:
        if schema_up not in _metadata_cache:
            _metadata_cache[schema_up] = _fetch_all_metadata_for_schema(schema_up)
    return _metadata_cache[schema_up].get(table_up, [])


def _fetch_all_metadata_for_schema(schema: str) -> dict:
    conn = get_oracle_connection()
    cursor = conn.cursor()
    metadata = {}
    try:
        # Inject schema as a string, no bind variables for identifiers
        sql = f"""
            SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.DATA_LENGTH, c.NULLABLE, c.DATA_PRECISION, c.DATA_SCALE, c.COLUMN_ID
            FROM ALL_TAB_COLUMNS c
            WHERE c.OWNER = '{schema}'
            AND c.TABLE_NAME IN (
                SELECT OBJECTNAME FROM {quote_identifier(schema)}.MAXOBJECT
                WHERE UPPER(OBJECTNAME) IN (
                    SELECT UPPER(TABLE_NAME) FROM ALL_TABLES WHERE OWNER = '{schema}'
                )
            )
            ORDER BY c.TABLE_NAME, c.COLUMN_ID
        """
        cursor.execute(sql)
        for row in cursor:
            table = row[0]
            metadata.setdefault(table, []).append({
                "column_name": row[1],
                "data_type": row[2],
                "data_length": row[3],
                "nullable": row[4],
                "data_precision": row[5],
                "data_scale": row[6]
            })
    except Exception as e:
        logger.error(f"Error fetching metadata for schema {schema}: {e}")
    finally:
        cursor.close()
        conn.close()
    return metadata














def get_table_count(schema: str, table: str) -> int:
    conn = get_oracle_connection()
    cursor = conn.cursor()
    try:
        sql = f'SELECT COUNT(*) FROM {quote_identifier(schema)}.{quote_identifier(table)}'
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    finally:
        cursor.close()
        conn.close()


def fetch_table_data_generator(schema: str, table: str, count_only=False, batch_size=1000):
    if count_only:
        return get_table_count(schema, table)

    conn = None
    cursor = None
    try:
        conn = get_oracle_connection()
        cursor = conn.cursor()

        # Fetch column info with proper string substitution (NO bind for identifiers)
        sql_cols = f"""
            SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{schema.upper()}'
            AND TABLE_NAME = '{table.upper()}'
            ORDER BY COLUMN_ID
        """
        cursor.execute(sql_cols)
        col_rows = cursor.fetchall()
        columns = [row[0] for row in col_rows]
        type_map = {row[0]: row[1] for row in col_rows}

        offset = 0
        while True:
            sql_data = f"""
                SELECT * FROM {quote_identifier(schema)}.{quote_identifier(table)}
                OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
            """
            cursor.execute(sql_data)
            rows = cursor.fetchall()
            if not rows:
                break

            batch = []
            for row in rows:
                batch.append({
                    col: clean_string_value(val, type_map.get(col))
                    for col, val in zip(columns, row)
                })
            yield batch

            offset += batch_size
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_sequences(schema: str) -> List[str]:
    conn = get_oracle_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT sequence_name FROM all_sequences WHERE sequence_owner = :owner
        """, {"owner": schema.upper()})
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def get_table_row_count(schema: str, table: str) -> int:
    return get_table_count(schema, table)


def fetch_indexes(schema: str):
    creds = load_credentials("oracle")
    conn = oracledb.connect(creds["username"], creds["password"], f"{creds['host']}:{creds['port']}/{creds['service']}")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT index_name, table_name FROM all_indexes WHERE owner = :owner AND index_type = 'NORMAL'
    """, [schema.upper()])
    indexes = [{"name": row[0], "table": row[1]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return indexes
