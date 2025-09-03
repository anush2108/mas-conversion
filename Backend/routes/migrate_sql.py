# routes/migrate_sql.py

from fastapi import APIRouter, Body
from typing import List
from services.schema_migrator import migrate_schema_fast, MigrationConfig
from services.sql_service import get_table_row_count
from services.sequence_sql_service import convert_sequences_from_mssql

router = APIRouter()

@router.post("/migrate/sql/schema")
def migrate_sql_schema_to_db2(schema_name: str, include_empty: bool = True):
    try:
        result = migrate_schema_fast(
            source_type="sql",
            source_schema=schema_name,
            table_filter=None,
            config=MigrationConfig()
        )
        return {
            "status": "success",
            "schema": schema_name,
            "tables_migrated": result.successful_migrations,
            "summary": result.__dict__,
            "table_results": [t.__dict__ for t in result.table_results]
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/migrate/sql/tables")
def migrate_selected_sql_tables(
    schema: str,
    tables: List[str] = Body(...),
    include_empty: bool = True
):
    if not tables:
        return {"status": "error", "message": "No tables provided for migration."}

    try:
        filtered_tables = []
        for table in tables:
            row_count = get_table_row_count(schema, table)
            if include_empty or row_count > 0:
                filtered_tables.append(table)

        if not filtered_tables:
            return {"status": "skipped", "message": "No non-empty tables to migrate."}

        result = migrate_schema_fast(
            source_type="sql",
            source_schema=schema,
            table_filter=None,
            config=MigrationConfig()
        )

        return {
            "status": "success",
            "schema": schema,
            "tables_migrated": result.successful_migrations,
            "summary": result.__dict__,
            "table_results": [t.__dict__ for t in result.table_results]
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/migrate/sql/sequences")
def migrate_sequences_from_sql(
    sql_details: dict = Body(...),
    db2_details: dict = Body(...),
    schema: str = Body(...)
):
    try:
        created_sequences = convert_sequences_from_mssql(sql_details, db2_details, schema)
        return {
            "status": "success",
            "message": f"{len(created_sequences)} sequences migrated from SQL Server to DB2.",
            "created_sequences": created_sequences
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
