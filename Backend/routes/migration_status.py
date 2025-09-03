# routes/migration_status.py

import os
import requests
from fastapi import APIRouter, HTTPException, Query
from routes.total_source_object import _get_oracle_totals, _get_mssql_totals, _normalize_schema

router = APIRouter(prefix="/migration-status", tags=["Migration Status"])

# Load CouchDB URL from environment with fallback for local dev
COUCHDB_URL = os.getenv("COUCHDB_URL")
TRANSACTION_DB_URL = f"{COUCHDB_URL}/transaction"

@router.get("/{transaction_id}")
def get_migration_status(
    transaction_id: str,
    source_type: str = Query(..., description="oracle or sql/sqlserver"),
    schema: str = Query(None, description="Schema/owner name, optional"),
    prefer_maximo_metadata: bool = Query(True, description="Use Maximo metadata"),
):
    """
    Retrieve migration progress for a given transaction ID.
    """

    resp = requests.get(f"{TRANSACTION_DB_URL}/{transaction_id}")
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Transaction not found")
    elif resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"CouchDB error: {resp.text}")

    doc = resp.json()
    status_data = doc.get("status", {}) or {}

    schema_from_doc = doc.get("schema")
    if not schema_from_doc:
        raise HTTPException(status_code=400, detail="Schema missing in transaction document")

    # Use schema from doc if not given in query params
    if not schema:
        schema = schema_from_doc

    st = source_type.lower()
    if st == "oracle":
        totals = _get_oracle_totals(schema, prefer_maximo_metadata)
    elif st in ("sql", "sqlserver"):
        totals = _get_mssql_totals(schema, prefer_maximo_metadata)
    else:
        raise HTTPException(status_code=400, detail="Unsupported source type")

    done_counts = {}
    total_done = 0
    total_expected = 0

    for obj_type, total_count in totals.items():
        successes = status_data.get(obj_type, {}).get("success", [])
        errors = status_data.get(obj_type, {}).get("error", [])

        count_success = len(successes)
        count_error = len(errors)

        done_counts[obj_type] = {
            "total": total_count,
            "success_count": count_success,
            "error_count": count_error,
            "errors": errors,
            "done": count_success,
            "percentage": round((count_success / total_count) * 100, 2) if total_count else 100,
        }

        total_done += count_success
        total_expected += total_count

    overall_percentage = round((total_done / total_expected) * 100, 2) if total_expected else 100

    return {
        "transaction_id": transaction_id,
        "schema": _normalize_schema(schema),
        "done_counts": done_counts,
        "overall": {
            "done": total_done,
            "total": total_expected,
            "percentage": overall_percentage,
        },
    }
