# utils/credentials_store.py

import json
import os
from typing import Dict, Optional

CREDENTIALS_FILE = "stored_credentials.json"

def load_all_credentials() -> Dict:
    if not os.path.exists(CREDENTIALS_FILE):
        return {}
    with open(CREDENTIALS_FILE, "r") as f:
        return json.load(f)

def save_credentials(source_type: str, creds: Dict, is_target: bool = False) -> None:
    data = load_all_credentials()
    if source_type not in data:
        data[source_type] = {}

    key = "target" if is_target else "source"
    data[source_type][key] = creds

    with open(CREDENTIALS_FILE, "w") as f:
        json.dump(data, f, indent=4)

def load_credentials(source_type: str, is_target: bool = False) -> Optional[Dict]:
    data = load_all_credentials()
    key = "target" if is_target else "source"
    return data.get(source_type, {}).get(key)

# ✅ Add these functions for validation service compatibility
def get_source_credentials(source_type: Optional[str] = None) -> Dict:
    for stype in ["oracle", "sql"]:
        if source_type and stype != source_type.lower():
            continue
        creds = load_credentials(stype, is_target=False)
        if creds:
            creds["source_type"] = stype
            return creds
    raise ValueError("No source credentials found.")


def get_target_credentials() -> Dict:
    data = load_all_credentials()
    for db_type in data:
        target_creds = data[db_type].get("target")
        if target_creds:
            return target_creds
    raise ValueError("No target (DB2) credentials found.")



