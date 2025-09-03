# routes/full_schema_migration.py

import asyncio
import logging
from typing import Optional
from utils.credentials_store import load_credentials , get_target_credentials

from fastapi import APIRouter, Query
from sse_starlette.sse import EventSourceResponse
from concurrent.futures import ThreadPoolExecutor

from services.schema_migrator import OptimizedSchemaMigrator
from services.sequence_oracle_service import convert_sequences_from_oracle
from services.sequence_sql_service import convert_sequences_from_mssql
from services.trigger_oracle_service import (
    fetch_triggers as fetch_oracle_triggers,
    migrate_trigger as migrate_oracle_trigger,
)
from services.trigger_sql_service import (
    fetch_triggers as fetch_sql_triggers,
    migrate_trigger as migrate_sql_trigger,
)
from services.index_oracle_service import get_index_ddl as oracle_index_ddl
from services.index_sql_service import get_index_ddl as sql_index_ddl
from services.index_converter import convert_index_ddl_to_db2, execute_index_ddl
from services.view_oracle_service import get_view_ddl as oracle_view_ddl
from services.view_sql_service import get_view_ddl as sql_view_ddl
from services.view_converter import convert_view_ddl_to_db2, execute_view_ddl
from services.db2_service import check_table_exists
from utils.couchdb_helpers import save_migration_status_to_couchdb
from utils.ddl_writer import save_ddl

router = APIRouter(prefix="/full-migration", tags=["FullSchemaMigration"])
logger = logging.getLogger(__name__)


def _msg(text: str) -> str:
    """Helper to format Server-Sent Event messages."""
    return f"data: {text}\n\n"


async def _async_wrap_sync_generator(sync_gen):
    """Wrap a synchronous generator for async SSE compatibility."""
    for item in sync_gen:
        yield item
        await asyncio.sleep(0)


@router.get("/all/stream")
async def migrate_full_schema_stream(
    source_type: str = Query(..., description="Source DB type: oracle or sqlserver"),
    schema: str = Query(..., description="Source schema name"),
    transaction_id: Optional[str] = Query(None, description="Transaction ID for migration tracking"),
    max_workers: int = Query(32, description="Max concurrency level for trigger migration"),
):
    """
    One-click migration of all schema objects: Tables, Sequences, Triggers, Indexes, Views.
    Streams progress updates in real time via SSE.
    """

    schema_migrator = OptimizedSchemaMigrator()

    async def generator():
        try:
            yield _msg(f"🚀 Starting full schema migration for schema '{schema}' from {source_type.upper()} (Transaction ID: {transaction_id})...")

            # --- 1. MIGRATE TABLES ---
            # Uncomment and implement table migration when ready
            yield _msg("🔄 Migrating tables...")
            sync_gen = schema_migrator.migrate_schema_streaming(
                source_type=source_type,
                source_schema=schema,
                target_schema=schema,
                table_filter=None,
                transaction_id=transaction_id,
            )
            async for msg in _async_wrap_sync_generator(sync_gen):
                yield _msg(msg)

            # --- 2. MIGRATE SEQUENCES ---
            # Uncomment and implement sequence migration when ready
            yield _msg("🔢 Migrating sequences...")
            source_creds = load_credentials(source_type)
            target_creds = get_target_credentials()
            if source_type.lower() == "oracle":
                sequences = convert_sequences_from_oracle(source_creds, target_creds, schema, transaction_id)
            else:
                sequences = convert_sequences_from_mssql(source_creds, target_creds, schema, transaction_id)
            
            if not sequences:
                yield _msg("⚠️ No sequences found to migrate.")
            else:
                for seq in sequences:
                    name = seq.get("sequence", "<unknown>")
                    if seq.get("created_in_db2"):
                        yield _msg(f"✅ Sequence '{name}' created.")
                    elif seq.get("skipped_existing"):
                        yield _msg(f"⚠️ Sequence '{name}' already exists — skipped.")
                    else:
                        yield _msg(f"❌ Failed to create sequence '{name}': {seq.get('error', 'Unknown error')}")

            # --- 3. MIGRATE TRIGGERS ---
            import time

            yield _msg("🔔 Migrating triggers...")
            time.sleep(3)  # pause for 3 seconds

            try:
                if source_type.lower() == "oracle":
                    triggers = fetch_oracle_triggers(schema)
                    migrate_func = migrate_oracle_trigger
                else:
                    triggers = fetch_sql_triggers(schema)
                    migrate_func = migrate_sql_trigger
            except Exception as e:
                yield _msg(f"❌ Error fetching triggers: {e}")
                triggers = []

            total = len(triggers)
            succeeded = 0

            loop = asyncio.get_event_loop()
            cached_check = lambda s, t: check_table_exists(s, t)

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                tasks = [
                    loop.run_in_executor(
                        executor,
                        migrate_func,
                        schema,
                        trig,
                        schema,  # target schema same as source
                        transaction_id,
                    )
                    for trig in triggers
                ]

                for future in asyncio.as_completed(tasks):
                    try:
                        result = await future
                        name = result.get("trigger", "<unknown>")
                        status = result.get("status")
                        reason = result.get("reason", "")

                        if status == "success":
                            succeeded += 1
                            yield _msg(f"✅ Trigger migrated: {name}")
                        elif status == "skipped":
                            yield _msg(f"⚠️ Trigger skipped: {name} - {reason}")
                        else:
                            yield _msg(f"❌ Trigger failed: {name} - {reason}")

                    except Exception as ex:
                        logger.error(f"Async trigger migration exception: {ex}")
                        yield _msg(f"❌ Unexpected error: {ex}")

            yield _msg(f"🎉 Trigger migration completed: {succeeded} / {total} migrated.")

            # # --- 4. MIGRATE INDEXES ---
            await asyncio.sleep(6)
            yield _msg("📐 Migrating indexes...")
            try:
                if source_type.lower() == "oracle":
                    indexes = oracle_index_ddl(schema)
                else:
                    indexes = sql_index_ddl(schema)
            except Exception as e:
                yield _msg(f"❌ Failed to fetch indexes: {e}")
                indexes = []

            for idx in indexes:
                name = idx.get("name")
                ddl = idx.get("source_ddl")
                table = idx.get("table")

                if not ddl:
                    yield _msg(f"⚠️ Index '{name}' skipped: No DDL.")
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [], "error": [name]}}, schema)
                    continue

                if table and not check_table_exists(schema, table):
                    yield _msg(f"⚠️ Index '{name}' skipped: Table '{table}' not found.")
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [], "error": [name]}}, schema)
                    continue

                try:
                    converted = convert_index_ddl_to_db2(ddl)
                    save_ddl("source", schema, name, ddl, object_type="index")
                    save_ddl("target", schema, name, converted, object_type="index")

                    if execute_index_ddl(converted, schema):
                        yield _msg(f"✅ Index '{name}' migrated.")
                        if transaction_id:
                            save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [name], "error": []}}, schema)
                    else:
                        yield _msg(f"❌ Failed to execute index '{name}'.")
                        if transaction_id:
                            save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [], "error": [name]}}, schema)
                except Exception as e:
                    yield _msg(f"❌ Error migrating index '{name}': {e}")
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [], "error": [name]}}, schema)

            # --- 5. MIGRATE VIEWS ---
            yield _msg("🪟 Migrating views...")
            try:
                if source_type.lower() == "oracle":
                    views = oracle_view_ddl(schema)
                else:
                    views = sql_view_ddl(schema)
            except Exception as e:
                yield _msg(f"❌ Failed to fetch views: {e}")
                views = []

            for view in views:
                name = view.get("name")
                ddl = view.get("source_ddl")

                if not ddl:
                    yield _msg(f"⚠️ View '{name}' skipped: No DDL")
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
                    continue

                # Extract referenced tables to check existence
                import re
                missing_tables = []
                matches = re.findall(r"\bFROM\s+([^\s,;]+)|\bJOIN\s+([^\s,;]+)", ddl, flags=re.IGNORECASE)
                tables_in_ddl = set()
                for m in matches:
                    tbl = m[0] or m[1]
                    if '.' in tbl:
                        tbl = tbl.split('.')[-1]
                    tables_in_ddl.add(tbl.upper())

                for tbl in tables_in_ddl:
                    if not check_table_exists(schema, tbl):
                        missing_tables.append(tbl)

                if missing_tables:
                    yield _msg(f"⚠️ View '{name}' skipped: Missing referenced tables {missing_tables}")
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
                    continue

                try:
                    converted = convert_view_ddl_to_db2(ddl)
                    save_ddl("source", schema, name, ddl, object_type="view")
                    save_ddl("target", schema, name, converted, object_type="view")

                    exec_success = False
                    for attempt in range(3):
                        if execute_view_ddl(converted, schema):
                            exec_success = True
                            break
                        await asyncio.sleep(2 ** attempt)

                    if exec_success:
                        yield _msg(f"✅ View '{name}' migrated.")
                        if transaction_id:
                            save_migration_status_to_couchdb(transaction_id, {"views": {"success": [name], "error": []}}, schema)
                    else:
                        yield _msg(f"❌ Failed to execute view '{name}'.")
                        if transaction_id:
                            save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)

                except Exception as e:
                    yield _msg(f"❌ Error migrating view '{name}': {e}")
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)

            yield _msg(f"🏁 Full schema migration completed for {schema} (Transaction ID: {transaction_id}).")

        except Exception as e:
            logger.error(f"Full schema migration failed: {e}")
            yield _msg(f"❌ Full schema migration error: {e}")

    return EventSourceResponse(generator())
