#routes/validate_data.py

from fastapi import APIRouter, HTTPException, Request
from services import validation_service

router = APIRouter()

@router.post("/validate/schema")
async def validate_entire_schema(request: Request):
    data = await request.json()
    source_type = data.get("source_type")
    schema = data.get("schema")

    if not source_type or not schema:
        raise HTTPException(status_code=400, detail="source_type and schema are required")

    return validation_service.validate_schema(source_type, schema)

@router.post("/validate/tables")
async def validate_selected_tables(request: Request):
    data = await request.json()
    tables = data.get("tables")
    source_type = data.get("source_type")
    if not tables or not source_type:
        raise HTTPException(status_code=400, detail="tables and source_type are required")
    return validation_service.validate_multiple_tables(tables, source_type)

@router.post("/validate/table")
async def validate_single_table(request: Request):
    data = await request.json()
    table_name = data.get("tables", [None])[0]
    source_type = data.get("source_type")
    if not table_name or not source_type:
        raise HTTPException(status_code=400, detail="table and source_type are required")
    return validation_service.validate_table(table_name, source_type)
