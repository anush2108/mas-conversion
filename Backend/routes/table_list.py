# routes/table_list.py
from fastapi import APIRouter, Query
from services.oracle_service import fetch_tables as fetch_oracle_tables
from services.sql_service import fetch_tables as fetch_sql_tables

router = APIRouter()

@router.get("/tables")
def get_tables(schema: str = Query(...), source: str = Query(...)):
    if source.lower() == "oracle":
        return fetch_oracle_tables(schema)
    elif source.lower() == "sql":
        return fetch_sql_tables(schema)
    else:
        return {"error": "Unsupported source"}
