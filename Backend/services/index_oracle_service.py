# services/index_oracle_service.py
from connections.oracle_connection import get_oracle_connection
from utils.credentials_store import get_source_credentials

def fetch_indexes(schema: str):
    creds = get_source_credentials("oracle")
    conn = get_oracle_connection(creds)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT NAME FROM {schema.upper()}.MAXSYSINDEXES")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

def get_index_ddl(schema: str, index_names=None):
    creds = get_source_credentials("oracle")
    conn = get_oracle_connection(creds)
    cursor = conn.cursor()
    try:
        if not index_names:
            cursor.execute(f"SELECT NAME FROM {schema.upper()}.MAXSYSINDEXES")
            index_names = [row[0] for row in cursor.fetchall()]
        if not index_names:
            return []
        bind_names = []
        params = {"schema": schema.upper()}
        for i, name in enumerate(index_names):
            bind_key = f"name{i}"
            bind_names.append(f":{bind_key}")
            params[bind_key] = name.upper()
        filter_clause = f"AND index_name IN ({', '.join(bind_names)})"
        sql = f"""
            SELECT index_name, dbms_metadata.get_ddl('INDEX', index_name, owner) AS ddl
            FROM all_indexes
            WHERE owner = :schema {filter_clause}
        """
        cursor.execute(sql, params)
        result = []
        for name, ddl_lob in cursor.fetchall():
            ddl = None
            # Convert LOB to string safely
            if ddl_lob is not None:
                try:
                    ddl = ddl_lob.read() if hasattr(ddl_lob, 'read') else str(ddl_lob)
                except Exception:
                    # fallback to str conversion if read() not available
                    ddl = str(ddl_lob)
            else:
                ddl = ''
            result.append({"name": name, "source_ddl": ddl})
        return result
    finally:
        cursor.close()
        conn.close()
