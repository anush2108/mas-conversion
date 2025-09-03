# services/index_converter.py
import re
import ibm_db
from utils.credentials_store import load_credentials
from services.db2_service import check_table_exists


def convert_index_ddl_to_db2(source_ddl: str) -> str:
    """
    Convert Oracle/SQL Server index DDL to DB2-compliant DDL.
    - Removes unsupported clauses (tablespace, storage, pctfree, etc.)
    - Normalizes quoting (Oracle, SQL Server → DB2)
    - Ensures schema/table/index names are quoted
    - Leaves only DB2-supported syntax

    Returns: Safe DB2 DDL string.
    """

    ddl = source_ddl.strip()

    # --- Remove Oracle-specific clauses ---
    ddl = re.sub(r"TABLESPACE\s+\"?[^\s\"]+\"?", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"STORAGE\s*\([^)]+\)", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"PCTFREE\s+\d+", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"INITRANS\s+\d+", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"MAXTRANS\s+\d+", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"NOPARALLEL", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"PARALLEL", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"COMPUTE STATISTICS", "", ddl, flags=re.IGNORECASE)

    # --- Remove SQL Server specific clauses ---
    ddl = re.sub(r"\[([^\]]+)\]", r'"\1"', ddl)  # Convert [col] → "col"
    ddl = re.sub(r"INCLUDE\s*\([^)]+\)", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"WITH\s*\([^)]+\)", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"ON\s+\[?[A-Za-z0-9_]+\]?\s+FILEGROUP\s+[A-Za-z0-9_\[\]]+", "", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"WHERE\s+.+$", "", ddl, flags=re.IGNORECASE)  # Filtered index not supported

    # --- Normalize whitespace ---
    ddl = re.sub(r"\s+", " ", ddl).strip()

    # --- Ensure CREATE INDEX format has quoted schema/table/index ---
    # Match CREATE [UNIQUE] INDEX index_name ON schema.table(cols)
    pattern = re.compile(
        r"CREATE\s+(UNIQUE\s+)?INDEX\s+\"?([\w\d_]+)\"?\s+ON\s+\"?([\w\d_]+)\"?\.\"?([\w\d_]+)\"?",
        re.IGNORECASE
    )
    m = pattern.search(ddl)
    if m:
        unique_kw = "UNIQUE " if m.group(1) else ""
        index_name = m.group(2).upper()
        schema_name = m.group(3).upper()
        table_name = m.group(4).upper()

        # Keep the rest of the statement after ON schema.table
        rest = ddl[m.end():].strip()

        # Safe DB2 DDL
        ddl = f'CREATE {unique_kw}INDEX "{index_name}" ON "{schema_name}"."{table_name}" {rest}'

    # --- Add semicolon if missing ---
    if not ddl.endswith(";"):
        ddl += ";"

    return ddl


def execute_index_ddl(ddl: str, schema: str, conn=None) -> bool:
    """
    Execute the given DB2 DDL string for creating an index.
    - Optionally accepts a pre-opened `conn` to avoid reconnect overhead in bulk migration.
    - Validates that the referenced table exists before attempting to create the index.
    """
    try:
        # --- Extract table from DDL for existence check ---
        match = re.search(r'ON\s+"?([\w\d_]+)"?\."?([\w\d_]+)"?', ddl, re.IGNORECASE)
        if not match:
            print(f"❌ Could not parse table from DDL, skipping: {ddl}")
            return False

        target_schema = match.group(1).upper()
        target_table = match.group(2).upper()

        if not check_table_exists(target_schema, target_table, skip_cache=True):
            print(f"❌ Skipped: Table {target_schema}.{target_table} does not exist in DB2.")
            return False

        # --- Open connection if not provided ---
        local_conn = None
        if conn is None:
            creds = load_credentials("db2", is_target=True)
            dsn = (
                f"DATABASE={creds['database']};HOSTNAME={creds['host']};PORT={creds['port']};"
                f"UID={creds['username']};PWD={creds['password']};SECURITY={creds.get('security', 'SSL')};"
                f"CHARSET=UTF-8;AUTOCOMMIT=0;CONNECTTIMEOUT=30;QUERYTIMEOUT=300;CURRENTSCHEMA={schema};"
            )
            local_conn = ibm_db.connect(dsn, "", "")
            conn_to_use = local_conn
        else:
            conn_to_use = conn

        print(f"[DEBUG] Executing Index DDL:\n{ddl}")
        ibm_db.exec_immediate(conn_to_use, ddl)

        if local_conn:
            ibm_db.commit(local_conn)
            ibm_db.close(local_conn)

        return True

    except Exception as e:
        print(f"❌ Index execution error: {e}")
        return False


def bulk_execute_index_ddls(ddls: list, schema: str) -> dict:
    """
    Execute many index DDLs efficiently using a single DB2 connection.
    Returns a dict with success and error lists.
    """
    results = {"success": [], "error": []}

    try:
        creds = load_credentials("db2", is_target=True)
        dsn = (
            f"DATABASE={creds['database']};HOSTNAME={creds['host']};PORT={creds['port']};"
            f"UID={creds['username']};PWD={creds['password']};SECURITY={creds.get('security', 'SSL')};"
            f"CHARSET=UTF-8;AUTOCOMMIT=0;CONNECTTIMEOUT=30;QUERYTIMEOUT=300;CURRENTSCHEMA={schema};"
        )
        conn = ibm_db.connect(dsn, "", "")

        for name, ddl in ddls:
            if execute_index_ddl(ddl, schema, conn=conn):
                results["success"].append(name)
            else:
                results["error"].append(name)

        ibm_db.commit(conn)
        ibm_db.close(conn)

    except Exception as e:
        print(f"❌ Bulk index migration connection error: {e}")

    return results
