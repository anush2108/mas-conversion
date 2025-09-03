# routes/views.py
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
import re
from sse_starlette.sse import EventSourceResponse

from services.view_oracle_service import (
    fetch_views as fetch_oracle_views,
    get_view_ddl as oracle_ddl
)
from services.view_sql_service import (
    fetch_views as fetch_sql_views,
    get_view_ddl as sql_ddl
)
from services.view_converter import convert_view_ddl_to_db2, execute_view_ddl
from services.db2_service import check_table_exists
from utils.ddl_writer import save_ddl
from utils.couchdb_helpers import save_migration_status_to_couchdb

router = APIRouter(prefix="/views", tags=["Views"])

@router.get("/list")
def list_views(source_type: str = Query(...), schema: str = Query(...)):
    try:
        if source_type.lower() == "oracle":
            return fetch_oracle_views(schema)
        elif source_type.lower() == "sqlserver":
            return fetch_sql_views(schema)
        else:
            raise HTTPException(status_code=400, detail="Invalid source_type")
    except Exception as e:
        logging.exception("Failed to list views")
        raise HTTPException(status_code=500, detail=str(e))


def extract_referenced_tables(ddl: str) -> List[str]:
    pattern = r"\bFROM\s+([^\s,;]+)|\bJOIN\s+([^\s,;]+)"
    matches = re.findall(pattern, ddl, re.IGNORECASE)
    tables = set()
    for m in matches:
        table = m[0] or m[1]
        if '.' in table:
            table = table.split('.')[-1]
        tables.add(table.upper())
    return list(tables)


def _msg(text: str) -> str:
    return f"data: {text}\n\n"


@router.post("/migrate")
def migrate_views(
    source_type: str = Query(...),
    target: str = Query(...),
    schema: str = Query(...),
    transaction_id: str = Query(...),   # transaction_id required
    view_names: Optional[List[str]] = Query(None)
):
    try:
        if source_type.lower() == "oracle":
            view_ddls = oracle_ddl(schema, view_names)
        elif source_type.lower() == "sqlserver":
            view_ddls = sql_ddl(schema, view_names)
        else:
            raise HTTPException(status_code=400, detail="Invalid source_type")

        migrated = []
        skipped = []

        for view in view_ddls:
            name = view["name"]
            source_ddl = view["source_ddl"]
            if not source_ddl:
                skipped.append({"view": name, "reason": "No source DDL"})
                continue

            # Check referenced tables exist in DB2
            missing_tables = []
            referenced_tables = extract_referenced_tables(source_ddl)
            for table in referenced_tables:
                if not check_table_exists(schema, table, skip_cache=True):
                    missing_tables.append(table)

            if missing_tables:
                skipped.append({"view": name, "reason": f"Missing tables: {', '.join(missing_tables)}"})
                # Update CouchDB skipped status incrementally
                save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
                continue

            try:
                target_ddl = convert_view_ddl_to_db2(source_ddl)
                save_ddl("source", schema, name, source_ddl, object_type="view")
                save_ddl("target", schema, name, target_ddl, object_type="view")

                if execute_view_ddl(target_ddl, schema):
                    migrated.append(name)
                    # Update CouchDB success status incrementally
                    save_migration_status_to_couchdb(transaction_id, {"views": {"success": [name], "error": []}}, schema)
                else:
                    skipped.append({"view": name, "reason": "Execution failed"})
                    save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
            except Exception as e:
                logging.exception(f"Failed to migrate view: {name}")
                skipped.append({"view": name, "reason": str(e)})
                save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)

        return {
            "migrated": migrated,
            "skipped": skipped,
            "total_requested": len(view_names or view_ddls),
            "total_migrated": len(migrated)
        }
    except Exception as e:
        logging.exception("View migration error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/migrate/stream")
async def migrate_views_stream(
    source_type: str = Query(...),
    schema: str = Query(...),
    target: str = Query("db2"),
    transaction_id: str = Query(...),
    view_names: Optional[List[str]] = Query(None)
):
    async def event_generator():
        yield _msg("🪟 Starting view migration...")

        try:
            view_ddls = oracle_ddl(schema, view_names) if source_type.lower() == "oracle" else sql_ddl(schema, view_names)
        except Exception as e:
            yield _msg(f"❌ Failed to fetch views: {e}")
            return

        if not view_ddls:
            yield _msg("❌ No views found to migrate.")
            return

        count = 0
        for view in view_ddls:
            name = view["name"]
            ddl = view["source_ddl"]
            if not ddl:
                yield _msg(f"⚠️ View '{name}' skipped: No DDL")
                save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
                continue

            missing_tables = []
            for table in extract_referenced_tables(ddl):
                if not check_table_exists(schema, table, skip_cache=True):
                    missing_tables.append(table)

            if missing_tables:
                yield _msg(f"⚠️ View '{name}' skipped: Missing tables {missing_tables}")
                save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
                continue

            try:
                converted = convert_view_ddl_to_db2(ddl)
                save_ddl("source", schema, name, ddl, object_type="view")
                save_ddl("target", schema, name, converted, object_type="view")

                if execute_view_ddl(converted, schema):
                    yield _msg(f"✅ View '{name}' migrated.")
                    count += 1
                    save_migration_status_to_couchdb(transaction_id, {"views": {"success": [name], "error": []}}, schema)
                else:
                    yield _msg(f"❌ Failed to execute view '{name}'")
                    save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)
            except Exception as e:
                yield _msg(f"❌ View '{name}' error: {e}")
                save_migration_status_to_couchdb(transaction_id, {"views": {"success": [], "error": [name]}}, schema)

        yield _msg(f"🎉 Views migration completed. Success: {count}/{len(view_ddls)}")

    return EventSourceResponse(event_generator())
