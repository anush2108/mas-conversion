# routes/override.py
from fastapi import APIRouter
from services.db2_service import check_table_exists, get_db2_connection
import ibm_db

router = APIRouter()

@router.post("/override-table/{source_type}/{schema}/{table}")
def override_table(source_type: str, schema: str, table: str):
    try:
        if check_table_exists(schema, table):
            conn = get_db2_connection()
            ibm_db.exec_immediate(conn, f'DROP TABLE "{schema}"."{table}"')
            ibm_db.close(conn)
            return {"status": "dropped", "message": f"{schema}.{table} dropped."}
        return {"status": "not_found", "message": f"{schema}.{table} not found in DB2."}
    except Exception as e:
        return {"status": "error", "message": str(e)}
