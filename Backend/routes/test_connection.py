# routes/test_connection.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from utils.credentials_store import save_credentials, load_credentials

import ibm_db
import oracledb
# oracledb.init_oracle_client(lib_dir=None)
import pyodbc

router = APIRouter()

class DBConnectionRequest(BaseModel):
    db_type: str
    host: str
    port: str
    username: str
    password: str
    database: Optional[str] = None
    sid: Optional[str] = None
    service_name: Optional[str] = None   
    connection_type: Optional[str] = "service_name"  # Default to service_name
    security: Optional[str] = None


def try_connect(details: DBConnectionRequest) -> dict:
    db_type = details.db_type.lower()
    try:
        if db_type == "oracle":
            # Handle both SID and Service Name connections
            if details.connection_type == "sid" and details.sid:
                # SID connection
                dsn = (
                    f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={details.host})(PORT={details.port}))"
                    f"(CONNECT_DATA=(SID={details.sid})))"
                )
                print(f"[🔌 ORACLE SID DSN] {dsn}")
            elif details.connection_type == "service_name" and details.service_name:
                # Service Name connection
                dsn = (
                    f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={details.host})(PORT={details.port}))"
                    f"(CONNECT_DATA=(SERVICE_NAME={details.service_name})))"
                )
                print(f"[🔌 ORACLE Service Name DSN] {dsn}")
            else:
                return {"status": "error", "message": "Either SID or SERVICE_NAME must be provided for Oracle."}

            # Attempt connection
            conn = oracledb.connect(user=details.username, password=details.password, dsn=dsn, mode=oracledb.DEFAULT_AUTH)
            
            # Test the connection with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return {"status": "success", "message": "Oracle connection successful."}

        elif db_type == "sql":
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={details.host};"
                f"DATABASE={details.database};"
                f"UID={details.username};"
                f"PWD={details.password};"
                f"PORT={details.port};"
                f"TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str)
            conn.close()

        elif db_type == "db2":
            dsn = (
                f"DATABASE={details.database};"
                f"HOSTNAME={details.host};"
                f"PORT={details.port};"
                f"UID={details.username};"
                f"PWD={details.password};"
                f"SECURITY={details.security or 'SSL'};"
            )
            conn = ibm_db.connect(dsn, "", "")
            ibm_db.close(conn)

        else:
            return {"status": "error", "message": f"Unsupported DB type: {db_type}"}

        return {"status": "success", "message": f"{db_type.upper()} connection successful."}

    except Exception as e:
        error_msg = str(e)
        print(f"[❌ CONNECTION ERROR] {error_msg}")
        
        # Provide more helpful error messages
        if "TNS" in error_msg or "could not resolve" in error_msg:
            return {"status": "error", "message": f"Connection failed: Cannot reach database server. Check host/port. Details: {error_msg}"}
        elif "invalid username/password" in error_msg.lower():
            return {"status": "error", "message": "Authentication failed: Invalid username or password."}
        elif "Listener" in error_msg:
            return {"status": "error", "message": f"Connection failed: Oracle listener not responding. Check if Oracle service is running. Details: {error_msg}"}
        else:
            return {"status": "error", "message": f"Connection failed: {error_msg}"}

@router.post("/test-connection")
def test_and_save_connection(details: DBConnectionRequest):
    db_type = details.db_type.lower()
    try:
        result = try_connect(details)
        if result["status"] == "success":
            save_credentials(db_type, details.dict(), is_target=(db_type == "db2"))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")


@router.get("/credentials/{db_type}")
def get_stored_credentials(db_type: str):
    db_type = db_type.lower()
    creds = load_credentials(db_type)
    if not creds:
        return JSONResponse(status_code=404, content={"error": f"No credentials found for '{db_type}'."})
    return creds
