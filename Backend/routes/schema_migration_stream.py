# routes/schema_migration_stream.py (FastAPI router code)
from fastapi import APIRouter, Query
from typing import List, Optional
from sse_starlette.sse import EventSourceResponse
from services.schema_migrator import OptimizedSchemaMigrator
from services.oracle_service import get_table_row_count as get_oracle_row_count
from services.sql_service import get_table_row_count as get_sql_row_count
from services.db2_service import cleanup_connections
import asyncio

router = APIRouter()
migrator = OptimizedSchemaMigrator()

async def async_wrap(sync_gen):
    for msg in sync_gen:
        yield msg
        await asyncio.sleep(0)

@router.get("/migrate-tables/{source_type}/{schema}/stream")
async def stream_selected_table_migration(
    source_type: str,
    schema: str,
    tables: List[str] = Query(...),
    include_empty: bool = Query(True),
    transaction_id: Optional[str] = Query(None),  # Important for CouchDB syncing
):
    if not tables:
        return EventSourceResponse(iter(["data: ❌ No tables provided.\n\n"]))

    try:
        get_row_count = get_oracle_row_count if source_type.lower() == "oracle" else get_sql_row_count

        # Filter tables based on include_empty flag
        filtered_tables = [t for t in tables if include_empty or get_row_count(schema, t) > 0]

        if not filtered_tables:
            return EventSourceResponse(iter(["data: ⚠️ No non-empty tables to migrate.\n\n"]))

        async def event_stream():
            try:
                sync_gen = migrator.migrate_schema_streaming(
                    source_type=source_type,
                    source_schema=schema,
                    target_schema=schema,
                    table_filter=filtered_tables,
                    transaction_id=transaction_id,  # Pass transaction id for incremental status saves
                )
                async for line in async_wrap(sync_gen):
                    yield f"data: {line}\n\n"
            except Exception as e:
                yield f"data: ❌ Error: {str(e)}\n\n"
            finally:
                cleanup_connections()

        return EventSourceResponse(event_stream())

    except Exception as e:
        return EventSourceResponse(iter([f"data: ❌ {str(e)}\n\n"]))
