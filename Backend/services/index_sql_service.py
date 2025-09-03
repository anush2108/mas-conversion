# services/index_sql_service.py
import pyodbc
from utils.credentials_store import load_credentials

def fetch_indexes(schema: str):
    creds = load_credentials("sql")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={creds['host']},{creds['port']};DATABASE={creds['database']};"
        f"UID={creds['username']};PWD={creds['password']}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        # Assuming you have a MAXSYSINDEXES equivalent in SQL Server schema, otherwise use sys.indexes
        cursor.execute(f"SELECT name FROM {schema}.MAXSYSINDEXES")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def get_index_ddl(schema: str, index_names=None):
    creds = load_credentials("sql")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={creds['host']},{creds['port']};DATABASE={creds['database']};"
        f"UID={creds['username']};PWD={creds['password']}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        if not index_names:
            cursor.execute(f"SELECT name FROM {schema}.MAXSYSINDEXES")
            index_names = [row[0] for row in cursor.fetchall()]
        if not index_names:
            return []
        placeholders = ','.join('?' for _ in index_names)
        sql = f"""
            SELECT i.name, i.type_desc,
            (SELECT definition FROM sys.index_columns ic
             JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
             WHERE ic.object_id = i.object_id FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') AS definition
            FROM sys.indexes i
            WHERE i.name IN ({placeholders}) AND SCHEMA_NAME(i.schema_id) = ?
        """
        params = index_names + [schema]
        cursor.execute(sql, params)
        result = []
        for name, type_desc, definition in cursor.fetchall():
            ddl = f"CREATE {type_desc} INDEX [{name}] ON [{schema}] ... "  # construct properly from definition; adjust as needed
            result.append({"name": name, "source_ddl": ddl})
        return result
    finally:
        cursor.close()
        conn.close()
