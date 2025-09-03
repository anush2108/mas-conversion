import os
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

from connections.oracle_connection import get_oracle_connection
from services.db2_service import check_schema_exists, create_schema_if_not_exists, execute_db2_ddl
from utils.ddl_writer import save_ddl
from utils.couchdb_helpers import save_migration_status_to_couchdb

DB2_MAX = 9223372036854775807


def ensure_ddl_dir():
    os.makedirs("generated_ddls", exist_ok=True)


def list_sequences_from_oracle(oracle_creds: dict, schema: str):
    """
    Fetch list of sequence names from the custom MAXSEQUENCE table in Oracle schema.
    """
    conn = get_oracle_connection(oracle_creds)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT SEQUENCENAME FROM {schema.upper()}.MAXSEQUENCE")
        sequences = [row[0] for row in cursor.fetchall()]
        return sequences
    finally:
        cursor.close()
        conn.close()


def convert_sequences_from_oracle(
    oracle_details: dict,
    db2_details: dict,
    schema: str,
    transaction_id: str = None  # Optional transaction_id for logging/status tracking
):
    """
    Convert and migrate sequences from Oracle to DB2,
    restricted to sequences listed in custom MAXSEQUENCE table.
    Performs per-sequence saving of migration status to CouchDB.
    """
    ensure_ddl_dir()
    sequences = []

    # 1. Get allowed sequences from MAXSEQUENCE table
    max_seq_list = list_sequences_from_oracle(oracle_details, schema)
    max_seq_set = set(name.upper() for name in max_seq_list)

    try:
        conn = get_oracle_connection(oracle_details)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT sequence_name, min_value, max_value, increment_by,
                   cycle_flag, order_flag, cache_size, last_number
            FROM all_sequences
            WHERE sequence_owner = :schema
            """,
            {"schema": schema.upper()},
        )
        all_sequence_rows = cursor.fetchall()
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()

    # 2. Filter only sequences present in MAXSEQUENCE
    sequence_rows = [row for row in all_sequence_rows if row[0].upper() in max_seq_set]

    final_schema = schema.upper()
    if not check_schema_exists(final_schema):
        if not create_schema_if_not_exists(final_schema):
            final_schema = db2_details.get("username", schema).upper()

    def process_sequence(row):
        name, min_val, max_val, inc, cycle, order, cache, last = row
        result = {"sequence": name}
        try:
            min_val = int(Decimal(min_val or 1))
            max_val = int(Decimal(max_val or DB2_MAX))
            inc = int(Decimal(inc or 1))
            cache = int(Decimal(cache or 1))
            last = int(Decimal(last or min_val))
            max_val = min(max_val, DB2_MAX)
            start_with = max(min(last + 1, max_val), min_val)

            if start_with > max_val:
                raise ValueError(f"START WITH ({start_with}) exceeds MAXVALUE ({max_val})")

            quoted_name = f'"{final_schema}"."{name}"'

            oracle_ddl = f"""CREATE SEQUENCE {schema}.{name}
START WITH {last}
INCREMENT BY {inc}
MINVALUE {min_val}
MAXVALUE {max_val}
{'CYCLE' if cycle == 'Y' else 'NOCYCLE'}
{'ORDER' if order == 'Y' else 'NOORDER'}
CACHE {cache};"""

            db2_ddl = f"""CREATE SEQUENCE {quoted_name} AS BIGINT
START WITH {start_with}
INCREMENT BY {inc}
MINVALUE {min_val}
MAXVALUE {max_val}
{'CYCLE' if cycle == 'Y' else 'NO CYCLE'}
CACHE {cache}
NO ORDER;"""

            save_ddl("source", schema, name, oracle_ddl, object_type="sequence")
            save_ddl("target", final_schema, name, db2_ddl, object_type="sequence")

            check_sql = f"SELECT 1 FROM SYSCAT.SEQUENCES WHERE SEQNAME = '{name.upper()}' AND SEQSCHEMA = '{final_schema}'"
            if not execute_db2_ddl(check_sql, expect_result=True):
                if not execute_db2_ddl(db2_ddl):
                    raise Exception(f"Execution failed for DB2 sequence: {final_schema}.{name}")
            else:
                result["skipped_existing"] = True

            result.update({
                "created_in_db2": True,
                "oracle_statement": oracle_ddl,
                "statement": db2_ddl,
                "oracle_current_value": last,
                "db2_start_value": start_with,
                "db2_schema": final_schema,
            })

            if transaction_id:
                # Save success immediately to CouchDB
                save_migration_status_to_couchdb(
                    transaction_id,
                    {"sequences": {"success": [name], "error": []}},
                    schema,
                )

        except Exception as e:
            result.update({
                "created_in_db2": False,
                "error": str(e),
            })

            if transaction_id:
                # Save error immediately to CouchDB
                save_migration_status_to_couchdb(
                    transaction_id,
                    {"sequences": {"success": [], "error": [name]}},
                    schema,
                )

        return result

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_sequence, row) for row in sequence_rows]
        for future in as_completed(futures):
            try:
                sequences.append(future.result())
            except Exception:
                # Optional: log here
                pass

    return sequences
