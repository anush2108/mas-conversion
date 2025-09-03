# routes/indexes.py
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import logging
from utils.ddl_writer import save_ddl

from services.index_oracle_service import fetch_indexes as fetch_oracle_indexes, get_index_ddl as oracle_ddl
from services.index_sql_service import fetch_indexes as fetch_sql_indexes, get_index_ddl as sql_ddl
from services.index_converter import convert_index_ddl_to_db2, execute_index_ddl
from utils.couchdb_helpers import save_migration_status_to_couchdb

router = APIRouter(prefix="/indexes", tags=["Indexes"])

@router.get("/list")
def list_indexes(source_type: str = Query(...), schema: str = Query(...)):
    try:
        if source_type.lower() == "oracle":
            return fetch_oracle_indexes(schema)
        elif source_type.lower() == "sqlserver":
            return fetch_sql_indexes(schema)
        else:
            raise HTTPException(status_code=400, detail="Invalid source_type")
    except Exception as e:
        logging.exception("Failed to list indexes")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/migrate")
def migrate_indexes(
    source_type: str = Query(...),
    target: str = Query(...),
    schema: str = Query(...),
    transaction_id: str = Query(...),
    index_names: Optional[List[str]] = Query(None)
):
    try:
        if source_type.lower() == "oracle":
            index_ddls = oracle_ddl(schema, index_names)
        elif source_type.lower() == "sqlserver":
            index_ddls = sql_ddl(schema, index_names)
        else:
            raise HTTPException(status_code=400, detail="Invalid source_type")

        migrated = []
        skipped = []

        for index in index_ddls:
            name = index["name"]
            source_ddl = index["source_ddl"]
            if not source_ddl:
                skipped.append({"index": name, "reason": "No source DDL"})
                continue

            try:
                target_ddl = convert_index_ddl_to_db2(source_ddl)
                # Save DDL files 
                save_ddl("source", schema, name, source_ddl, object_type="index")
                save_ddl("target", schema, name, target_ddl, object_type="index")

                if execute_index_ddl(target_ddl, schema):
                    migrated.append(name)
                    save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [name], "error": []}}, schema)
                else:
                    skipped.append({"index": name, "reason": "Execution failed"})
                    save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [], "error": [name]}}, schema)
            except Exception as e:
                logging.exception(f"Failed to migrate index: {name}")
                skipped.append({"index": name, "reason": str(e)})
                save_migration_status_to_couchdb(transaction_id, {"indexes": {"success": [], "error": [name]}}, schema)

        return {
            "migrated": migrated,
            "skipped": skipped,
            "total_requested": len(index_names or index_ddls),
            "total_migrated": len(migrated)
        }

    except Exception as e:
        logging.exception("Index migration error")
        raise HTTPException(status_code=500, detail=str(e))
