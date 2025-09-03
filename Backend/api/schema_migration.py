# ─── FILE: api/schema_migration.py ──────────────────────────────────

from fastapi import APIRouter, Body, HTTPException
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.schema_migrator import migrate_schema_parallel
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

class SchemaMigrationRequest(BaseModel):
    source_type: str
    schema_name: str
    use_same_schema: bool
    new_schema_name: Optional[str] = None
    max_workers: int = 8

@router.post("/migrate-schema")
def migrate_schema_controller(payload: SchemaMigrationRequest):
    try:
        result = migrate_schema_parallel(
            source_type=payload.source_type,
            source_schema=payload.schema_name,
            use_same_schema=payload.use_same_schema,
            new_schema_name=payload.new_schema_name,
            max_workers=payload.max_workers
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
