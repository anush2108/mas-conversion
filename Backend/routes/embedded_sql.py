
# === routes/embedded_sql.py ===

from fastapi import APIRouter, Query
from pydantic import BaseModel
from services.embedded_sql_service import (
    fetch_all_schemas,
    fetch_existing_table_columns,
    fetch_matching_rows,
)
from services.watsonx_emb_sql import call_model  # Your conversion function

router = APIRouter()

class ConvertRequest(BaseModel):
    table: str
    column: str
    value: str

@router.get("/schema")
def get_schemas():
    return fetch_all_schemas()

@router.get("/embedded_sqltable")
def get_allowed_table_columns():
    return fetch_existing_table_columns()

@router.get("/embedded_sqltable/rows")
def get_rows(
    table: str = Query(..., description="Table name"),
    function_name: str = Query(..., description="SQL keyword")
):
    return fetch_matching_rows(table, function_name)

@router.post("/embedded_sqltable/convert")
def convert_to_db2(req: ConvertRequest):
    try:
        converted_sql = call_model(req.value)  # Convert Oracle SQL to DB2 SQL
        return {
            "table": req.table,
            "column": req.column,
            "original": req.value,
            "converted": converted_sql
        }
    except Exception as e:
        return {
            "table": req.table,
            "column": req.column,
            "original": req.value,
            "converted": "",
            "error": str(e)
        }
