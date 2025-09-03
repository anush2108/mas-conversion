import os
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

from connections.sql_connection import get_sql_connection
from services.db2_service import check_schema_exists, create_schema_if_not_exists, execute_db2_ddl
from utils.ddl_writer import save_ddl
from utils.couchdb_helpers import save_migration_status_to_couchdb

DB2_MAX = 9223372036854775807


def list_sequences_from_mssql(sql_creds: dict, schema: str):
    """
    Fetch the list of sequence names from the custom MAXSEQUENCE table in SQL Server schema.
    """
    conn = get_sql_connection(sql_creds)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT SEQUENCENAME FROM {schema}.MAXSEQUENCE")
        sequences = [row[0] for row in cursor.fetchall()]
        return sequences
    finally:
        cursor.close()
        conn.close()


def get_sequence_metadata(seq_name: str, schema: str, sql_creds: dict):
    """
    Fetch actual sequence metadata from SQL Server sys.sequences.
    """
    conn = get_sql_connection(sql_creds)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT name, CAST(start_value AS BIGINT), CAST(increment AS BIGINT),
                   CAST(minimum_value AS BIGINT), CAST(maximum_value AS BIGINT),
                   is_cycling, cache_size, CAST(current_value AS BIGINT)
            FROM sys.sequences
            WHERE name = ? AND schema_id = SCHEMA_ID(?)
            """,
            (seq_name, schema),
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"Metadata not found for sequence {seq_name}")
        return row
    finally:
        cursor.close()
        conn.close()


def convert_sequences_from_mssql(
    sql_creds: dict,
    db2_creds: dict,
    schema: str,
    transaction_id: str = None
):
    """
    Convert and migrate sequences defined in MAXSEQUENCE from MS SQL Server to Db2.
    """
    sequence_names = list_sequences_from_mssql(sql_creds, schema)
    if not sequence_names:
        return []

    final_schema = schema.upper()
    if not check_schema_exists(final_schema):
        if not create_schema_if_not_exists(final_schema):
            final_schema = db2_creds.get("username", final_schema)

    results = []

    def process(seq_name):
        result = {"sequence": seq_name}
        try:
            meta = get_sequence_metadata(seq_name, schema, sql_creds)
            (name, start_val, inc, min_val, max_val, cycle, cache, current_val) = meta

            inc = int(inc or 1)
            # Ensure cache size at least 1 for Db2 compatibility
            cache = max(int(cache or 20), 1)
            min_val = int(min_val or 1)
            max_val = min(int(max_val or DB2_MAX), DB2_MAX)
            current_val = int(current_val or start_val or min_val)

            start_with = current_val + inc
            if start_with < min_val:
                start_with = min_val
            if start_with > max_val:
                raise ValueError(f"Start value {start_with} exceeds max {max_val}")

            quoted_name = f'"{final_schema}"."{name.upper()}"'

            oracle_ddl = f'-- Sequence {name} migrated from MSSQL MAXSEQUENCE and sys.sequences'

            cache_clause = f"CACHE {cache}" if cache > 0 else ""

            db2_ddl = f"""CREATE SEQUENCE {quoted_name} AS BIGINT
START WITH {start_with}
INCREMENT BY {inc}
MINVALUE {min_val}
MAXVALUE {max_val}
{cache_clause};"""

            save_ddl("source", schema, name, oracle_ddl, "sequence")
            save_ddl("target", final_schema, name, db2_ddl, "sequence")

            check_sql = f"SELECT 1 FROM syscat.sequences WHERE seqname = '{name.upper()}' AND seqschema = '{final_schema}'"
            exists = execute_db2_ddl(check_sql, expect_result=True)

            if exists and len(exists) > 0:
                result["skipped"] = True
                result["created"] = False
            else:
                if not execute_db2_ddl(db2_ddl):
                    raise RuntimeError(f"Failed to create sequence {name} in Db2")
                result["created"] = True

            result.update({
                "db2_ddl": db2_ddl,
                "start_with": start_with,
                "schema": final_schema,
                "cache": cache,
                "increment": inc,
                "minvalue": min_val,
                "maxvalue": max_val,
            })

            if transaction_id:
                save_migration_status_to_couchdb(
                    transaction_id,
                    {
                        "sequences": {
                            "success": [name] if result["created"] or result.get("skipped") else [],
                            "error": [] if result["created"] or result.get("skipped") else [name],
                        }
                    },
                    schema,
                )

        except Exception as e:
            result.update({"created": False, "error": str(e)})
            if transaction_id:
                save_migration_status_to_couchdb(
                    transaction_id,
                    {"sequences": {"success": [], "error": [seq_name]}},
                    schema,
                )

        return result

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(process, seq_name) for seq_name in sequence_names]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception:
                pass

    return results
