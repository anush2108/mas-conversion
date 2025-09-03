import logging
import oracledb
# oracledb.init_oracle_client(lib_dir=None)
from fastapi import HTTPException
from connections.oracle_connection import get_oracle_connection
from utils.credentials_store import get_source_credentials
import requests
import os

# IBM ML Deployment Config (use env vars, fallback to defaults)
IBM_API_KEY = os.getenv("IBM_API_KEY", "w1Dr0mhKlkBqCWi_T8fq8N_F472Y5cRxpIjJ8xh8xE_0")
DEPLOYMENT_URL = os.getenv(
    "IBM_DEPLOYMENT_URL",
    "https://us-south.ml.cloud.ibm.com/ml/v4/deployments/0e5906c0-c5ea-4c4f-bf59-e30e450d763a/predictions?version=2021-05-01"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------- Oracle DB ----------------
def get_oracledb_connection():
    creds = get_source_credentials("oracle")
    return get_oracle_connection(creds)


def fetch_blobclob_count_from_oracle(conn, schema: str):
    """Fetch total count of rows containing BLOB/CLOB in a schema."""
    cursor = None
    try:
        safe_schema = schema.strip().upper()
        if not safe_schema.replace("_", "").isalnum():
            raise HTTPException(status_code=400, detail="Invalid schema name.")

        cursor = conn.cursor()
        plsql = f"""
        DECLARE
          CURSOR lob_tables_cur IS
            SELECT DISTINCT UPPER(m.OBJECTNAME) AS table_name
            FROM {safe_schema}.MAXOBJECT m
            WHERE UPPER(m.OBJECTNAME) IN (
              SELECT table_name
              FROM all_tab_columns
              WHERE owner = '{safe_schema}'
              AND data_type IN ('BLOB', 'CLOB')
            );
          TYPE lob_col_tab IS TABLE OF VARCHAR2(128);
          lob_cols     lob_col_tab;
          v_sql        CLOB;
          lob_count    NUMBER;
          total_count  NUMBER := 0;
        BEGIN
          FOR tbl IN lob_tables_cur LOOP
            BEGIN
              SELECT column_name
              BULK COLLECT INTO lob_cols
              FROM all_tab_columns
              WHERE owner = '{safe_schema}'
              AND table_name = tbl.table_name
              AND data_type IN ('BLOB', 'CLOB');

              v_sql := 'SELECT COUNT(*) FROM {safe_schema}.' || tbl.table_name || ' WHERE ';
              FOR i IN 1 .. lob_cols.COUNT LOOP
                IF i > 1 THEN
                  v_sql := v_sql || ' OR ';
                END IF;
                v_sql := v_sql || lob_cols(i) || ' IS NOT NULL';
              END LOOP;

              EXECUTE IMMEDIATE v_sql INTO lob_count;
              total_count := total_count + lob_count;
            EXCEPTION
              WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error in table ' || tbl.table_name || ': ' || SQLERRM);
            END;
          END LOOP;
          :result := total_count;
        END;
        """
        result = cursor.var(oracledb.NUMBER)
        cursor.execute(plsql, result=result)
        value = result.getvalue()
        return int(value or 0)

    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=500, detail=f"Oracle DB error: {error_obj.message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting BLOB/CLOB count: {str(e)}")
    finally:
        if cursor:
            cursor.close()


def fetch_oracle_values(schema: str):
    """Fetch schema metrics (scalars) from Oracle."""
    conn = None
    cursor = None
    try:
        safe_schema = schema.strip().upper()
        if not safe_schema.replace("_", "").isalnum():
            raise HTTPException(status_code=400, detail="Invalid schema name.")

        conn = get_oracledb_connection()
        cursor = conn.cursor()
        query = f"""
            SELECT
                (SELECT ROUND(SUM(bytes) / 1024 / 1024 / 1024, 2) FROM user_segments) AS data_volume_gb,
                (SELECT COUNT(*) FROM {safe_schema}.MAXOBJECT) AS num_tables,
                (SELECT COUNT(*) FROM {safe_schema}.WORKORDER) AS workorder_records,
                (SELECT COUNT(*) FROM {safe_schema}.MAXSYSINDEXES) AS num_indexes
            FROM dual
        """
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No data found.")

        # ✅ fix: use indices, not strings
        data_volume_gb = float(row[0]) if row[0] is not None else 0.0
        num_tables = int(row[1]) if row[1] is not None else 0
        workorder_records = int(row[2]) if row[2] is not None else 0
        num_indexes = int(row[3]) if row[3] is not None else 0
        blobclob_records = fetch_blobclob_count_from_oracle(conn, safe_schema)

        result = {
            "data_volume_gb": data_volume_gb,
            "num_tables": num_tables,
            "workorder_records": workorder_records,
            "num_indexes": num_indexes,
            "blobclob_records": blobclob_records,
        }
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------- IBM Model ----------------
def get_ibm_token(api_key: str) -> str:
    """Get IAM token from IBM Cloud."""
    token_url = "https://iam.cloud.ibm.com/identity/token"
    response = requests.post(token_url, data={
        "apikey": api_key,
        "grant_type": "urn:ibm:params:oauth:grant-type:apikey"
    })
    response.raise_for_status()
    token_json = response.json()
    if "access_token" not in token_json:
        raise HTTPException(status_code=500, detail="IBM token invalid format")
    return token_json["access_token"]


def call_ibm_model(input_data: dict):
    """Call IBM ML model with Oracle metrics."""
    try:
        token = get_ibm_token(IBM_API_KEY)

        fields = [
            "blobclob_records",
            "data_volume_gb",
            "num_indexes",
            "num_tables",
            "workorder_records"
        ]
        values = [[
            input_data.get("blobclob_records", 0),
            input_data.get("data_volume_gb", 0),
            input_data.get("num_indexes", 0),
            input_data.get("num_tables", 0),
            input_data.get("workorder_records", 0)
        ]]

        # ✅ ensure scalars
        for val in values[0]:
            if isinstance(val, (list, tuple, dict)):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid input value type: {type(val)} in {val}"
                )

        payload = {
            "input_data": [
                {"fields": fields, "values": values}
            ]
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.post(DEPLOYMENT_URL, json=payload, headers=headers)
        response.raise_for_status()
        prediction = response.json()

        # ✅ fix: validate IBM response is dict
        if not isinstance(prediction, dict):
            raise HTTPException(status_code=500, detail="Invalid IBM ML response format")

        return prediction

    except requests.exceptions.HTTPError as e:
        try:
            return {"error": response.json(), "status_code": response.status_code}
        except Exception:
            raise HTTPException(status_code=response.status_code, detail=response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"IBM model error: {str(e)}")
