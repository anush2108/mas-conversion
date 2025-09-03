# services/ai_complexity_sql.py
import logging
import pyodbc
import requests
from fastapi import HTTPException
from utils.credentials_store import load_credentials

# ---------------- IBM ML Deployment Config ----------------
IBM_API_KEY = "mcHk8CXMTx1hZ4WVHZGDjL3RqEJZKrURynWnA78iD"
DEPLOYMENT_URL = "https://us-south.ml.cloud.ibm.com/ml/v4/deployments/cb8a1fb7-f806-469c-b6dd-7ab34e85001c/predictions?version=2021-05-01"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_mssql_connection():
    """Establish MSSQL connection using stored credentials."""
    creds = load_credentials("sql")
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={creds['host']},{creds['port']};DATABASE={creds['database']};"
            f"UID={creds['username']};PWD={creds['password']}"
        )
        conn = pyodbc.connect(conn_str)
        logging.info("✅ Connected to MSSQL successfully")
        return conn
    except Exception as e:
        logging.error(f"❌ MSSQL connection error: {e}")
        raise HTTPException(status_code=500, detail="MSSQL connection failed")


def fetch_lob_tables(schema: str):
    """
    Fetch tables & columns with MSSQL BLOB/CLOB-like types in given schema.
    Schema is passed as user input.
    """
    query_tables = """
    SELECT DISTINCT t.name AS table_name, c.name AS column_name
    FROM sys.tables t
    INNER JOIN sys.columns c ON t.object_id = c.object_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = ?
      AND (
            ty.name IN ('varbinary', 'varchar', 'nvarchar')
            AND (c.max_length = -1) -- -1 means MAX type in SQL Server
          )
    ORDER BY t.name;
    """
    try:
        conn = get_mssql_connection()
        cursor = conn.cursor()
        cursor.execute(query_tables, (schema,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{"table_name": row.table_name, "column_name": row.column_name} for row in results]
    except Exception as e:
        logging.error(f"❌ Error fetching LOB tables for schema '{schema}': {e}")
        raise HTTPException(status_code=500, detail="Error fetching LOB tables")


def fetch_mssql_values(schema: str):
    """
    Fetch MSSQL database metrics for ML prediction.
    Includes blob/clob equivalent record count from schema.
    """
    conn = None
    cursor = None
    try:
        conn = get_mssql_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                (SELECT SUM(reserved_page_count)*8/1024 FROM sys.dm_db_partition_stats) AS data_volume_mb,
                (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE') AS num_tables,
                (SELECT COUNT(*) FROM WorkOrder) AS workorder_records,
                (SELECT COUNT(*) FROM sys.indexes) AS num_indexes
        """)
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No data found in MSSQL.")

        # Count blob/clob equivalent columns from schema
        blobclob_count = len(fetch_lob_tables(schema))

        result = {
            "data_volume_gb": round(row[0] / 1024, 2) if row[0] else 0,
            "num_tables": row[1],
            "workorder_records": row[2],
            "num_indexes": row[3],
            "blobclob_records": blobclob_count
        }
        logging.info("📦 MSSQL metrics: %s", result)
        return result

    except Exception as e:
        logging.error(f"❌ Error fetching MSSQL metrics: {e}")
        raise HTTPException(status_code=500, detail=f"MSSQL query error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_ibm_token(api_key: str) -> str:
    token_url = "https://iam.cloud.ibm.com/identity/token"
    try:
        response = requests.post(token_url, data={
            "apikey": api_key,
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey"
        })
        response.raise_for_status()
        token = response.json()["access_token"]
        logging.info("🔐 IBM access token retrieved")
        return token
    except Exception as e:
        logging.error(f"❌ Error fetching IBM token: {e}")
        raise HTTPException(status_code=500, detail=f"IBM token error: {str(e)}")


def call_ibm_model(input_data: dict):
    try:
        token = get_ibm_token(IBM_API_KEY)
        payload = {
            "input_data": [
                {
                    "fields": [
                        "blobclob_records",
                        "data_volume_gb",
                        "num_indexes",
                        "num_tables",
                        "workorder_records"
                    ],
                    "values": [[
                        input_data["blobclob_records"],
                        input_data["data_volume_gb"],
                        input_data["num_indexes"],
                        input_data["num_tables"],
                        input_data["workorder_records"]
                    ]]
                }
            ]
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        logging.info("📤 Sending payload to IBM ML model: %s", payload)
        response = requests.post(DEPLOYMENT_URL, json=payload, headers=headers)
        response.raise_for_status()
        prediction = response.json()
        logging.info("✅ IBM ML prediction: %s", prediction)
        return prediction
    except Exception as e:
        logging.error(f"❌ Error calling IBM ML model: {e}")
        raise HTTPException(status_code=500, detail=f"IBM model error: {str(e)}")
