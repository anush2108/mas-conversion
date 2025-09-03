# services/view_sql_service.py
import pyodbc
from utils.credentials_store import load_credentials

def fetch_views(schema: str):
    """Fetch view names from MAXVIEW table."""
    creds = load_credentials("sql")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={creds['host']},{creds['port']};DATABASE={creds['database']};"
        f"UID={creds['username']};PWD={creds['password']}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT VIEWNAME FROM {schema}.MAXVIEW")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def get_view_ddl(schema: str, view_names=None):
    creds = load_credentials("sql")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={creds['host']},{creds['port']};DATABASE={creds['database']};"
        f"UID={creds['username']};PWD={creds['password']}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    try:
        if not view_names:
            cursor.execute(f"SELECT VIEWNAME FROM {schema}.MAXVIEW")
            view_names = [row[0] for row in cursor.fetchall()]
        if not view_names:
            return []
        placeholders = ','.join('?' for _ in view_names)
        sql = f"""
            SELECT v.name, m.definition
            FROM sys.views v
            JOIN sys.sql_modules m ON v.object_id = m.object_id
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = ? AND v.name IN ({placeholders})
        """
        params = [schema] + view_names
        cursor.execute(sql, params)
        result = []
        for name, body in cursor.fetchall():
            ddl = f'CREATE VIEW [{schema}].[{name}] AS\n{body}'
            result.append({"name": name, "source_ddl": ddl})
        return result
    finally:
        cursor.close()
        conn.close()
