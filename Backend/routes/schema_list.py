# routes/schema_list.py
from fastapi import APIRouter
from services.oracle_service import fetch_schemas as fetch_oracle_schemas
from services.sql_service import fetch_schemas as fetch_sql_schemas

router = APIRouter()

@router.get("/o_schemas")
def list_oracle_schemas():
    return fetch_oracle_schemas()

@router.get("/s_schemas")
def list_sql_schemas():
    return fetch_sql_schemas()
