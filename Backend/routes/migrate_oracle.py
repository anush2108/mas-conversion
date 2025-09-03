
# routes/migrate_oracle.py
from fastapi import APIRouter, Body
from typing import List
from services.schema_migrator import migrate_schema_fast, MigrationConfig, TableMigrationResult

from services.oracle_service import get_table_row_count
from services.sequence_oracle_service import convert_sequences_from_oracle

router = APIRouter()

@router.post("/migrate/oracle/schema")
def migrate_oracle_schema_to_db2(schema_name: str, include_empty: bool = True):
    try:
        result = migrate_schema_fast(
            source_type="oracle",
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

@router.post("/migrate/oracle/tables")
def migrate_selected_oracle_tables(
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
            source_type="oracle",
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

@router.post("/migrate/oracle/sequences")
def migrate_sequences_from_oracle(
    oracle_details: dict = Body(...),
    db2_details: dict = Body(...),
    schema: str = Body(...)
):
    try:
        created_sequences = convert_sequences_from_oracle(oracle_details, db2_details, schema)
        return {
            "status": "success",
            "message": f"{len(created_sequences)} sequences migrated from Oracle to DB2.",
            "created_sequences": created_sequences
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
