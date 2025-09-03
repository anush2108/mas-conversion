# services/trigger_converter.py
import re
import ibm_db
import time
from typing import Callable, Optional, Dict
from utils.credentials_store import get_target_credentials
from utils.ddl_writer import save_ddl
from utils.couchdb_helpers import save_migration_status_to_couchdb

def execute_db2_trigger_ddl(trigger_ddl: str, max_retries: int = 3) -> bool:
    for attempt in range(1, max_retries + 1):
        try:
            creds = get_target_credentials()
            dsn = (
                f"DATABASE={creds['database']};"
                f"HOSTNAME={creds['host']};"
                f"PORT={creds['port']};"
                f"PROTOCOL=TCPIP;"
                f"UID={creds['username']};"
                f"PWD={creds['password']};"
                f"SECURITY={creds.get('security', 'SSL')};"
            )
            print(f"[DB2 Connection Attempt {attempt}] Connect {creds['host']}:{creds['port']}")
            conn = ibm_db.connect(dsn, "", "")
            ibm_db.set_option(conn, {ibm_db.SQL_ATTR_AUTOCOMMIT: ibm_db.SQL_AUTOCOMMIT_OFF}, 1)

            try:
                result = ibm_db.exec_immediate(conn, trigger_ddl)
            except Exception as err:
                err_msg = ibm_db.stmt_errmsg()
                print(f"[DB2 Trigger Execution Error] {err_msg or err}")
                ibm_db.rollback(conn)
                ibm_db.close(conn)
                if 'timeout' in (err_msg or '').lower() and attempt < max_retries:
                    delay = 2 ** attempt
                    print(f"[Retrying after {delay}s due to timeout]")
                    time.sleep(delay)
                    continue
                return False

            if result:
                ibm_db.commit(conn)
                ibm_db.close(conn)
                print("[DB2 Trigger Created Successfully]")
                return True
            else:
                err_msg = ibm_db.stmt_errmsg()
                print(f"[DB2 Trigger Failed] {err_msg}")
                ibm_db.rollback(conn)
                ibm_db.close(conn)
                if 'timeout' in (err_msg or '').lower() and attempt < max_retries:
                    delay = 2 ** attempt
                    print(f"[Retrying after {delay}s due to timeout]")
                    time.sleep(delay)
                    continue
                return False

        except Exception as e:
            print(f"[DB2 Connect/Execute Exception] {e}")
            if 'timeout' in str(e).lower() and attempt < max_retries:
                delay = 2 ** attempt
                print(f"[Retrying after {delay}s due to exception]")
                time.sleep(delay)
                continue
            return False

    print("[DB2 Trigger Creation Failed After Retries]")
    return False

def convert_oracle_to_db2(schema: str, trigger_name: str, oracle_ddl: str) -> str:
    timing_match = re.search(r'\b(BEFORE|AFTER|INSTEAD OF)\b', oracle_ddl, re.IGNORECASE)
    timing = timing_match.group(1).upper() if timing_match else "AFTER"
    events = list({ev.upper() for ev in re.findall(r'\b(INSERT|UPDATE|DELETE)\b', oracle_ddl, re.IGNORECASE)})
    event_clause = " OR ".join(events) if events else "INSERT"
    table_match = re.search(r'ON\s+(\w+)', oracle_ddl, re.IGNORECASE)
    table_name = table_match.group(1).upper() if table_match else "<MISSING_TABLE>"

    body_match = re.search(r'BEGIN(.*?)END;', oracle_ddl, re.IGNORECASE | re.DOTALL)
    if not body_match:
        raise ValueError(f"Trigger {trigger_name} body not found in Oracle DDL")
    body = body_match.group(1).strip()

    body = re.sub(r':NEW\.', 'NEW_TEMP.', body, flags=re.IGNORECASE)
    body = re.sub(r':OLD\.', 'OLD_TEMP.', body, flags=re.IGNORECASE)
    body = re.sub(r'SELECT\s+(\w+)\.NEXTVAL\s+INTO\s+(\w+)\s+FROM\s+DUAL;',
                  lambda m: f"SELECT NEXT VALUE FOR {schema}.{m.group(1)} INTO {m.group(2)} FROM SYSIBM.SYSDUMMY1;",
                  body,
                  flags=re.IGNORECASE)
    body = re.sub(r'(\b\w+\b)\s*:=\s*([^;]+);', r'SET \1 = \2;', body, flags=re.IGNORECASE)
    body = re.sub(r'(NEW_TEMP|OLD_TEMP)\.(\w+)\s*=\s*([^;]+);?', r'SET \1.\2 = \3;', body, flags=re.IGNORECASE)
    body = re.sub(r'NEW_TEMP\.', 'NEW_ROW.', body)
    body = re.sub(r'OLD_TEMP\.', 'OLD_ROW.', body)
    body = re.sub(r'NEW_ROW\.SET\s+(\w+)\s*=\s*([^;]+);?', r'SET NEW_ROW.\1 = \2;', body, flags=re.IGNORECASE)
    body = re.sub(r'OLD_ROW\.SET\s+(\w+)\s*=\s*([^;]+);?', r'SET OLD_ROW.\1 = \2;', body, flags=re.IGNORECASE)

    uses_nextval = re.search(r'NEXTVAL', body, re.IGNORECASE)
    if uses_nextval:
        body = "DECLARE NEXTVAL INTEGER;\n    " + body

    statements = [stmt.strip() for stmt in body.split(';') if stmt.strip()]
    body = ';\n    '.join(statements)
    if not body.endswith(';'):
        body += ';'

    referencing = []
    if "INSERT" in events or "UPDATE" in events:
        referencing.append("NEW AS NEW_ROW")
    if "DELETE" in events or "UPDATE" in events:
        referencing.append("OLD AS OLD_ROW")
    referencing_clause = f"REFERENCING {' '.join(referencing)}" if referencing else ""

    ddl = (
        f"CREATE OR REPLACE TRIGGER {schema}.{trigger_name}\n"
        f"{timing} {event_clause} ON {schema}.{table_name}\n"
        f"{referencing_clause}\n"
        f"FOR EACH ROW\n"
        f"BEGIN\n"
        f"    {body}\n"
        f"END;\n"
    )
    return ddl

def convert_sql_to_db2(schema: str, trigger_name: str, sql_ddl: str) -> str:
    body = sql_ddl.strip()
    body = re.sub(r'INSERTED\.', 'NEW.', body, flags=re.IGNORECASE)
    body = re.sub(r'DELETED\.', 'OLD.', body, flags=re.IGNORECASE)
    body = re.sub(r'\bGO\b', '', body, flags=re.IGNORECASE)

    timing_match = re.search(r'\b(AFTER|INSTEAD OF|BEFORE)\b', sql_ddl, re.IGNORECASE)
    event_match = re.search(r'\b(INSERT|UPDATE|DELETE)\b', sql_ddl, re.IGNORECASE)
    table_match = re.search(r'ON\s+(\w+)', sql_ddl, re.IGNORECASE)
    timing = timing_match.group(1).upper() if timing_match else "AFTER"
    event = event_match.group(1).upper() if event_match else "INSERT"
    table_name = table_match.group(1).upper() if table_match else "<MISSING_TABLE>"

    body_match = re.search(r'BEGIN(.*?)END', sql_ddl, re.IGNORECASE | re.DOTALL)
    if body_match:
        body = body_match.group(1).strip()

    ddl = (
        f"CREATE TRIGGER {schema}.{trigger_name}\n"
        f"{timing} {event} ON {schema}.{table_name}\n"
        f"REFERENCING NEW AS NEW OLD AS OLD\n"
        f"FOR EACH ROW MODE DB2\n"
        f"BEGIN ATOMIC\n"
        f"    {body}\n"
        f"END"
    )
    return ddl

def migrate_single_trigger(
    source_type: str,
    schema: str,
    trigger_name: str,
    fetch_trigger_ddl_func: Callable,
    convert_func: Callable,
    check_table_exists_func: Callable,
    save_ddl_func: Callable,
    execute_ddl_func: Callable,
    transaction_id: Optional[str] = None,
    max_retries: int = 3,
) -> Dict:
    try:
        ddl = fetch_trigger_ddl_func(schema, trigger_name)
        if not ddl:
            if transaction_id:
                save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger_name]}}, schema)
            return {"trigger": trigger_name, "status": "skipped", "reason": "DDL not found"}

        converted = convert_func(schema, trigger_name, ddl)

        match = re.search(r'ON\s+(?:"?(\w+)"?\.)?"?(\w+)"?', converted, re.IGNORECASE)
        tgt_schema = match.group(1).upper() if match and match.group(1) else schema.upper()
        tgt_table = match.group(2).upper() if match else None

        if not tgt_table:
            if transaction_id:
                save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger_name]}}, schema)
            return {"trigger": trigger_name, "status": "skipped", "reason": "Target table name not found in DDL"}

        if not check_table_exists_func(tgt_schema, tgt_table):
            if transaction_id:
                save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger_name]}}, schema)
            return {"trigger": trigger_name, "status": "skipped", "reason": f"Table {tgt_schema}.{tgt_table} not found in DB2"}

        save_ddl_func("source", schema, trigger_name, ddl, object_type="trigger")
        save_ddl_func("target", tgt_schema, trigger_name, converted, object_type="trigger")

        for attempt in range(1, max_retries + 1):
            if execute_ddl_func(converted):
                if transaction_id:
                    save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [trigger_name], "error": []}}, schema)
                return {"trigger": trigger_name, "status": "success"}
            else:
                delay = 2 ** attempt
                print(f"[Attempt {attempt}] Failed to execute trigger {trigger_name} in DB2, retrying after {delay}s.")
                time.sleep(delay)

        if transaction_id:
            save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger_name]}}, schema)
        return {"trigger": trigger_name, "status": "failed", "reason": "Failed after retries"}

    except Exception as e:
        if transaction_id:
            save_migration_status_to_couchdb(transaction_id, {"triggers": {"success": [], "error": [trigger_name]}}, schema)
        return {"trigger": trigger_name, "status": "error", "reason": str(e)}
