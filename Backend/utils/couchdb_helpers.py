# utils/couchdb_helpers.py
import os
import requests
import logging
import time
from utils.credentials_store import get_source_credentials

logger = logging.getLogger(__name__)

# Load from env
COUCHDB_USER = os.getenv("COUCHDB_USER", "admin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "admin")
COUCHDB_URL = os.getenv("COUCHDB_URL", f"http://{COUCHDB_USER}:{COUCHDB_PASSWORD}@localhost:5984")

def save_migration_status_to_couchdb(transaction_id: str, new_status_update: dict, schema: str, max_retries=5):
    """
    Saves migration status updates to CouchDB, merging with existing document if present.
    """
    doc_id = transaction_id
    couchdb_url = f"{COUCHDB_URL}/transaction/{doc_id}"
    # Detect and store source_type from credentials
    try:
        source_creds = get_source_credentials()
        source_type = source_creds.get("source_type", "").lower()
    except Exception:
        source_type = None

    for attempt in range(max_retries):
        try:
            get_resp = requests.get(couchdb_url)
            
            if get_resp.status_code == 200:
                existing_doc = get_resp.json()
                existing_status = existing_doc.get("status", {})

                for obj_type, results in new_status_update.items():
                    if obj_type not in existing_status:
                        existing_status[obj_type] = {"success": [], "error": []}

                    existing_success = set(existing_status[obj_type].get("success", []))
                    existing_error = set(existing_status[obj_type].get("error", []))

                    # Success handling
                    for obj_name in results.get("success", []):
                        existing_error.discard(obj_name)
                        existing_success.add(obj_name)

                    # Error handling
                    for obj_name in results.get("error", []):
                        existing_success.discard(obj_name)
                        existing_error.add(obj_name)

                    existing_status[obj_type]["success"] = sorted(existing_success)
                    existing_status[obj_type]["error"] = sorted(existing_error)

                data = {
                    "_id": doc_id,
                    "_rev": existing_doc.get("_rev"),
                    "schema": schema,
                    "status": existing_status,
                    "source_type": source_type or existing_doc.get("source_type"),  # persist source_type
                }

            elif get_resp.status_code == 404:
                # Create new doc
                data = {
                    "_id": doc_id,
                    "schema": schema,
                    "status": new_status_update,
                    "source_type": source_type,  # save source_type on insert
                }
            else:
                logger.warning(f"Unexpected GET from CouchDB: {get_resp.status_code} {get_resp.text}")
                return

            # Save to CouchDB
            put_resp = requests.put(couchdb_url, json=data)
            if put_resp.status_code in (200, 201):
                logger.info(f"✅ Migration status saved to CouchDB for transaction {transaction_id}")
                return
            elif put_resp.status_code == 409:
                logger.warning(f"⚠️ CouchDB update conflict attempt {attempt+1}, retrying...")
                time.sleep(0.1)
                continue
            else:
                logger.error(f"❌ Failed to save migration status: {put_resp.status_code} {put_resp.text}")
                return

        except Exception as e:
            logger.error(f"🔥 Error saving migration status to CouchDB: {e}")
            return

    logger.error(f"❌ Failed to save migration status after {max_retries} retries due to conflicts.")
