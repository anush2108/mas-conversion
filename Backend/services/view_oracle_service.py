# services/view_oracle_service.py
from connections.oracle_connection import get_oracle_connection
from utils.credentials_store import get_source_credentials

def fetch_views(schema: str):
    """Return view names from MAXVIEW.VIEWNAME only."""
    creds = get_source_credentials("oracle")
    conn = get_oracle_connection(creds)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT VIEWNAME FROM {schema.upper()}.MAXVIEW")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def get_view_ddl(schema: str, view_names=None):
    creds = get_source_credentials("oracle")
    conn = get_oracle_connection(creds)
    cursor = conn.cursor()
    try:
        if not view_names:
            cursor.execute(f"SELECT VIEWNAME FROM {schema.upper()}.MAXVIEW")
            view_names = [row[0] for row in cursor.fetchall()]
        if not view_names:
            return []
        bind_names = []
        params = {"schema": schema.upper()}
        for i, name in enumerate(view_names):
            bind_key = f"name{i}"
            bind_names.append(f":{bind_key}")
            params[bind_key] = name.upper()
        filter_clause = f"AND view_name IN ({', '.join(bind_names)})"
        sql = f"""
            SELECT view_name, text
            FROM all_views
            WHERE owner = :schema {filter_clause}
        """
        cursor.execute(sql, params)
        result = []
        for name, body in cursor.fetchall():
            ddl = f'CREATE OR REPLACE VIEW "{schema.upper()}"."{name}" AS\n{body}'
            result.append({"name": name, "source_ddl": ddl})
        return result
    finally:
        cursor.close()
        conn.close()
