# routes/triggers.py
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
from functools import lru_cache
from utils.couchdb_helpers import save_migration_status_to_couchdb
from utils.ddl_writer import save_ddl
from services.trigger_converter import (
    convert_oracle_to_db2,
    convert_sql_to_db2,
    execute_db2_trigger_ddl,
    migrate_single_trigger,
)
from services.trigger_oracle_service import (
    fetch_triggers as fetch_oracle_triggers,
    fetch_trigger_definition as oracle_trigger_ddl,
)
from services.trigger_sql_service import (
    fetch_triggers as fetch_sql_triggers,
    fetch_trigger_definition as sql_trigger_ddl,
)
from services.db2_service import check_table_exists
from sse_starlette.sse import EventSourceResponse

router = APIRouter(prefix="/triggers", tags=["Triggers"])
logger = logging.getLogger(__name__)

def _msg(text: str) -> str:
    return f"data: {text.strip()}\n\n"

def cache_table_check():
    @lru_cache(maxsize=None)
    def cached_check(schema: str, table: str) -> bool:
        return check_table_exists(schema, table)
    return cached_check

@router.get("/list")
def list_triggers(
    source_type: str = Query(..., description="Source DB type"),
    schema: str = Query(..., description="Source schema name"),
) -> List[str]:
    try:
        if source_type.lower() == "oracle":
            return fetch_oracle_triggers(schema)
        elif source_type.lower() == "sqlserver":
            return fetch_sql_triggers(schema)
        else:
            raise HTTPException(status_code=400, detail="Unsupported source_type")
    except Exception as e:
        logger.error(f"Failed to fetch triggers: {e}")
        raise

@router.post("/migrate")
def migrate_triggers_parallel(
    source_type: str = Query(...),
    target: str = Query(...),
    schema: str = Query(...),
    trigger_names: Optional[List[str]] = Query(None),
    max_workers: int = Query(32),
    transaction_id: str = Query(...),
) -> Dict:
    if source_type.lower() == "oracle":
        fetch_func = fetch_oracle_triggers
        ddl_func = oracle_trigger_ddl
        convert_func = convert_oracle_to_db2
    elif source_type.lower() == "sqlserver":
        fetch_func = fetch_sql_triggers
        ddl_func = sql_trigger_ddl
        convert_func = convert_sql_to_db2
    else:
        raise HTTPException(status_code=400, detail="Unsupported source_type")

    all_triggers = fetch_func(schema)

    if trigger_names:
        allowed = {t.upper() for t in trigger_names}
        filtered_triggers = [
            t for t in all_triggers if (t.upper() if isinstance(t, str) else t.get("trigger_name", "").upper()) in allowed
        ]
    else:
        filtered_triggers = all_triggers

    canonical_names = []
    for t in filtered_triggers:
        if isinstance(t, str):
            canonical_names.append(t)
        elif isinstance(t, dict):
            canonical_names.append(t.get("trigger_name") or t.get("trigger"))

    cached_check = cache_table_check()
    migrated = []
    skipped = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                migrate_single_trigger,
                source_type,
                schema,
                trig,
                ddl_func,
                convert_func,
                cached_check,
                save_ddl,
                execute_db2_trigger_ddl,
                transaction_id,
                3,
            )
            for trig in canonical_names
        ]
        for f in as_completed(futures):
            try:
                result = f.result()
                if result["status"] in ("success", "migrated"):
                    migrated.append(result["trigger"])
                else:
                    skipped.append(result)
            except Exception as e:
                logger.error(f"Trigger migration future error: {e}")

    return {
        "total_requested": len(canonical_names),
        "total_migrated": len(migrated),
        "migrated": migrated,
        "skipped": skipped,
    }

@router.get("/migrate/stream")
async def migrate_triggers_stream_parallel(
    source_type: str = Query(...),
    target: str = Query("db2"),
    schema: str = Query(...),
    trigger_names: Optional[List[str]] = Query(None),
    max_workers: int = Query(32),
    transaction_id: Optional[str] = Query(None),
):
    async def stream():
        yield _msg("Starting trigger migration in parallel...")

        if source_type.lower() == "oracle":
            fetch_func = fetch_oracle_triggers
            ddl_func = oracle_trigger_ddl
            convert_func = convert_oracle_to_db2
        elif source_type.lower() == "sqlserver":
            fetch_func = fetch_sql_triggers
            ddl_func = sql_trigger_ddl
            convert_func = convert_sql_to_db2
        else:
            yield _msg("Unsupported source_type")
            return

        all_triggers = fetch_func(schema)

        if trigger_names:
            allowed = {t.upper() for t in trigger_names}
            filtered_triggers = [
                t for t in all_triggers if (t.upper() if isinstance(t, str) else t.get("trigger_name", "").upper()) in allowed
            ]
        else:
            filtered_triggers = all_triggers

        canonical_names = []
        for t in filtered_triggers:
            if isinstance(t, str):
                canonical_names.append(t)
            elif isinstance(t, dict):
                canonical_names.append(t.get("trigger_name") or t.get("trigger"))

        succeeded = 0
        total = len(canonical_names)
        cached_check = cache_table_check()
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            tasks = [
                loop.run_in_executor(
                    pool,
                    migrate_single_trigger,
                    source_type,
                    schema,
                    trig,
                    ddl_func,
                    convert_func,
                    cached_check,
                    save_ddl,
                    execute_db2_trigger_ddl,
                    transaction_id,
                    3,
                )
                for trig in canonical_names
            ]

            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    name = result.get("trigger", "<unknown>")
                    if result.get("status") in ("success", "migrated"):
                        succeeded += 1
                        yield _msg(f"✅ Migrated: {name}")
                    else:
                        reason = result.get("reason") or result.get("message") or "Unknown"
                        yield _msg(f"⛔ Skipped {name}: {reason}")
                except Exception as ex:
                    logger.error(f"Async trigger migration exception: {ex}")
                    yield _msg(f"❌ Unexpected error: {ex}")

        yield _msg(f"Migration completed. Success: {succeeded}/{total}")

    return EventSourceResponse(stream())
