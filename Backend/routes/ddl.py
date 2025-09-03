# routes/ddl.py
from fastapi import APIRouter, Query, HTTPException
import os
router = APIRouter()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
@router.get("/ddl/targets")
async def list_targets():
    """Returns available migration targets."""
    return ["source", "target"]
@router.get("/ddl/schemas")
async def list_schemas(target: str = Query(..., description="Target database")):
    target_dir = os.path.join(BASE_DIR, f"generated_ddls/{target}")
    if not os.path.exists(target_dir):
        raise HTTPException(status_code=404, detail=f"Target '{target}' not found")
    schemas = [name for name in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, name))]
    return {"schemas": schemas}
@router.get("/ddl/objects")
async def list_objects(
    target: str = Query(...),
    schema: str = Query(...),
    object_type: str = Query(...),
):
    folder_type_map = {
        "table": "tables",
        "tables": "tables",
        "sequence": "sequences",   # FIXED
        "sequences": "sequences",  # FIXED
        "trigger": "triggers",
        "triggers": "triggers",
        "index": "index",
        "indexes": "index",
        "view": "views",
        "views": "views"
    }
    folder = folder_type_map.get(object_type.lower())
    if not folder:
        raise HTTPException(status_code=400, detail="Invalid or missing object_type")
    base_path = os.path.join(BASE_DIR, f"generated_ddls/{target}/{schema}")
    if not os.path.exists(base_path):
        raise HTTPException(status_code=404, detail=f"Schema '{schema}' not found under target '{target}'")
    search_path = os.path.join(base_path, folder)
    if not os.path.exists(search_path):
        return {"objects": []}
    files = [f for f in os.listdir(search_path) if f.lower().endswith(".sql")]
    objects = [os.path.splitext(f)[0] for f in files]
    return {"objects": objects}
@router.get("/ddl/object_ddl")
async def get_object_ddl(
    target: str = Query(...),
    schema: str = Query(...),
    object_type: str = Query(...),
    object_name: str = Query(...),
):
    folder_type_map = {
        "table": "tables",
        "tables": "tables",
        "sequence": "sequences",   # FIXED
        "sequences": "sequences",  # FIXED
        "trigger": "triggers",
        "triggers": "triggers",
        "index": "index",
        "indexes": "index",
        "view": "views",
        "views": "views"
    }
    folder = folder_type_map.get(object_type.lower())
    if not folder:
        raise HTTPException(status_code=400, detail="Invalid object_type")
    ddl_path = os.path.join(BASE_DIR, f"generated_ddls/{target}/{schema}/{folder}/{object_name}.sql")
    if not os.path.isfile(ddl_path):
        raise HTTPException(status_code=404, detail="DDL file not found for the specified object")
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_content = f.read()
    return {"ddl": ddl_content}