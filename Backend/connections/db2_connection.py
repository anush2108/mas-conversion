# connections/db2_connection.py
import ibm_db
import ibm_db_dbi

def test_db2_connection_custom(details):
    dsn = (
        f"DATABASE={details.database};"
        f"HOSTNAME={details.host};"
        f"PORT={details.port};"
        f"UID={details.username};"
        f"PWD={details.password};"
        f"SECURITY={details.security or 'SSL'};"
    )
    try:
        conn = ibm_db.connect(dsn, "", "")
        ibm_db.close(conn)
        return {"status": "success", "message": "DB2 connection successful."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_db2_connection(details):
    dsn = (
        f"DATABASE={details['database']};"
        f"HOSTNAME={details['host']};"
        f"PORT={details['port']};"
        f"UID={details['username']};"
        f"PWD={details['password']};"
        f"SECURITY={details.get('security', 'SSL')};"
    )
    ibm_conn = ibm_db.connect(dsn, "", "")
    return ibm_db_dbi.Connection(ibm_conn)
