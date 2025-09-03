# services/trigger_sql_service.py
import re
from typing import Optional, List
from connections.sql_connection import get_sql_connection
from services.trigger_converter import convert_sql_to_db2, execute_db2_trigger_ddl
from utils.ddl_writer import save_ddl
from utils.couchdb_helpers import save_migration_status_to_couchdb
from services.db2_service import check_table_exists
from utils.credentials_store import get_source_credentials


def fetch_triggers(schema: str) -> List[str]:
    creds = get_source_credentials("sql")
    conn = get_sql_connection(creds)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT t.name
            FROM sys.triggers t
            JOIN sys.tables tb ON tb.object_id = t.parent_id
            JOIN MAXIMO.MAXOBJECT mo ON mo.OBJECTNAME = tb.name
            WHERE SCHEMA_NAME(tb.schema_id) = ?
            ORDER BY tb.name, t.name
        """, (schema,))
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def fetch_trigger_definition(schema: str, trigger: str) -> Optional[str]:
    creds = get_source_credentials("sql")
    conn = get_sql_connection(creds)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT OBJECT_DEFINITION(OBJECT_ID(?))
        """, (f"{schema}.{trigger}",))
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    finally:
        cursor.close()
        conn.close()


def migrate_trigger(schema: str, trigger: str, target_schema: str, transaction_id: Optional[str] = None) -> dict:
    print(f"Starting migration of SQL Server trigger {trigger} from {schema} to {target_schema}")

    ddl = fetch_trigger_definition(schema, trigger)
    if not ddl:
        if transaction_id:
            save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger]}}, schema)
        return {"trigger": trigger, "status": "failed", "reason": "Trigger DDL not found"}

    try:
        db2_ddl = convert_sql_to_db2(target_schema, trigger, ddl)
    except Exception as e:
        if transaction_id:
            save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger]}}, schema)
        return {"trigger": trigger, "status": "failed", "reason": f"Conversion failed: {e}"}

    match = re.search(r'ON\s+(?:"?(\w+)"?\.)?"?(\w+)"?', db2_ddl, re.IGNORECASE)
    tgt_schema = match.group(1).upper() if match and match.group(1) else target_schema.upper()
    tgt_table = match.group(2).upper() if match else None

    if not tgt_table or not check_table_exists(tgt_schema, tgt_table):
        if transaction_id:
            save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger]}}, schema)
        return {"trigger": trigger, "status": "skipped", "reason": f"Target table {tgt_schema}.{tgt_table} missing in DB2"}

    save_ddl("source", schema, trigger, ddl, object_type="trigger")
    save_ddl("target", tgt_schema, trigger, db2_ddl, object_type="trigger")

    if not execute_db2_trigger_ddl(db2_ddl):
        if transaction_id:
            save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger]}}, schema)
        return {"trigger": trigger, "status": "failed", "reason": "Failed to execute DB2 trigger"}

    if transaction_id:
        save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [trigger], "error": []}}, schema)

    return {"trigger": trigger, "status": "success"}
