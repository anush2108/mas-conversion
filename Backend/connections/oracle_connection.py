# connections/oracle_connection.py
import oracledb

def test_oracle_connection_custom(details):
    if getattr(details, "sid", None):
        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={details.host})(PORT={details.port}))(CONNECT_DATA=(SID={details.sid})))"
    elif getattr(details, "service", None):
        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={details.host})(PORT={details.port}))(CONNECT_DATA=(SERVICE_NAME={details.service})))"
    else:
        return {"status": "error", "message": "Missing SID or SERVICE_NAME in Oracle connection details."}

    try:
        print(f"[🔌 Oracle Test DSN] {dsn}")
        conn = oracledb.connect(user=details.username, password=details.password, dsn=dsn)
        conn.close()
        return {"status": "success", "message": "Oracle DB connection successful."}
    except Exception as e:
        return {"status": "error", "message": str(e)}




import oracledb
from utils.credentials_store import load_credentials

def get_oracle_connection(details=None):
    # Load credentials if not passed explicitly
    if not details:
        details = load_credentials("oracle")
    
    if not details:
        raise ValueError("Oracle connection details not provided.")

    # Build DSN for SID or Service Name
    if details.get("sid"):
        dsn = (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={details['host']})(PORT={details['port']}))"
            f"(CONNECT_DATA=(SID={details['sid']})))"
        )
    elif details.get("service_name") or details.get("service"):
        dsn = (
            f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={details['host']})(PORT={details['port']}))"
            f"(CONNECT_DATA=(SERVICE_NAME={details.get('service_name') or details.get('service')})))"
        )
    else:
        dsn = oracledb.makedsn(details["host"], int(details["port"]), sid=details.get("sid"))

    return oracledb.connect(user=details["username"], password=details["password"], dsn=dsn)
