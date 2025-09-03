# routes/current_migration.py
import os
import requests
from fastapi import APIRouter, HTTPException
from utils.credentials_store import get_source_credentials

router = APIRouter(prefix="/current-migration", tags=["Migration Status"])

# Load CouchDB URL from environment
COUCHDB_URL = os.getenv("COUCHDB_URL")
if not COUCHDB_URL:
    raise RuntimeError("COUCHDB_URL is not set in environment variables")

TRANSACTION_DOCS_URL = f"{COUCHDB_URL}/transaction/_all_docs?include_docs=true"


@router.get("")
def get_current_migration():
    """
    Returns the most recent migration job still in progress from CouchDB.
    - Reads directly from transaction docs
    - Ensures schema and source_type are present
    - Uses status_flag if present OR falls back to counts-based logic
    """
    try:
        resp = requests.get(TRANSACTION_DOCS_URL)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch transactions from CouchDB: {resp.text}"
            )

        data = resp.json()
        running = []

        for row in data.get("rows", []):
            doc = row.get("doc") or {}
            schema = doc.get("schema")
            transaction_id = doc.get("_id")
            source_type = doc.get("source_type")
            status_data = doc.get("status", {})
            status_flag = doc.get("status_flag", "running")  # defaults to "running"

            # Skip docs missing required info
            if not schema or not isinstance(status_data, dict) or not status_data:
                continue

            # Ensure source_type exists (fallback to stored credentials)
            if not source_type:
                try:
                    creds = get_source_credentials()
                    source_type = creds.get("source_type")
                except Exception:
                    source_type = None

            # Old count-based detection
            total_done = sum(len(v.get("success", [])) for v in status_data.values())
            total_expected = sum(
                len(v.get("success", [])) + len(v.get("error", []))
                for v in status_data.values()
            )

            # Detect running:
            # 1. Explicit status_flag not marked "completed"
            # 2. Or old count-based check passes
            if status_flag != "completed" or total_expected == 0 or total_done < total_expected:
                running.append({
                    "transaction_id": transaction_id,
                    "schema": schema,
                    "source_type": source_type,
                    "status": "running"
                })

        if not running:
            raise HTTPException(status_code=404, detail="No running migration found")

        # Return latest running migration document (last in query result)
        return running[-1]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
