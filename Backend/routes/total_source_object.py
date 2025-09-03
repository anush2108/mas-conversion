import logging
import oracledb
from typing import Dict
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
from utils.credentials_store import load_credentials

router = APIRouter(prefix="/total-source-objects", tags=["Totals"])
logger = logging.getLogger(__name__)


class TotalsResponse(BaseModel):
    source_type: str
    schema_name: str = Field(..., alias="schema")
    totals: Dict[str, int]

    class Config:
        allow_population_by_field_name = True


def _normalize_schema(schema: str) -> str:
    return schema.strip().upper()


def get_oracle_connection():
    """
    Returns a connection to Oracle using python-oracledb in THIN mode (no Instant Client required).
    """
    creds = load_credentials("oracle", is_target=False)
    if not creds:
        raise ValueError("Oracle credentials not found.")

    CONNECT_TIMEOUT = 999999
    RECV_TIMEOUT = 999999

    if creds.get("sid"):
        dsn = (
            f"(DESCRIPTION="
            f"(ADDRESS=(PROTOCOL=TCP)(HOST={creds['host']})(PORT={creds['port']})"
            f"(CONNECT_TIMEOUT={CONNECT_TIMEOUT})(RECV_TIMEOUT={RECV_TIMEOUT}))"
            f"(CONNECT_DATA=(SID={creds['sid']}))"
            f")"
        )
    elif creds.get("service_name"):
        dsn = (
            f"(DESCRIPTION="
            f"(ADDRESS=(PROTOCOL=TCP)(HOST={creds['host']})(PORT={creds['port']})"
            f"(CONNECT_TIMEOUT={CONNECT_TIMEOUT})(RECV_TIMEOUT={RECV_TIMEOUT}))"
            f"(CONNECT_DATA=(SERVICE_NAME={creds['service_name']}))"
            f")"
        )
    else:
        raise HTTPException(status_code=400, detail="Missing service_name or sid for Oracle connection.")

    return oracledb.connect(
        user=creds["username"],
        password=creds["password"],
        dsn=dsn,
        encoding="UTF-8"
    )


def _mssql_connect(creds):
    import pyodbc
    driver = creds.get("driver", "{ODBC Driver 17 for SQL Server}")
    conn_str = (
        f"DRIVER={driver};SERVER={creds['host']},{creds['port']};"
        f"DATABASE={creds['database']};UID={creds['user']};PWD={creds['password']};TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str)


def _get_oracle_totals(schema: str, prefer_maximo_metadata: bool) -> Dict[str, int]:
    conn = get_oracle_connection()
    cur = conn.cursor()
    totals = {"tables": 0, "sequences": 0, "triggers": 0, "indexes": 0, "views": 0}
    sch = _normalize_schema(schema)

    try:
        if prefer_maximo_metadata:
            cur.execute(f'SELECT COUNT(OBJECTNAME) FROM {sch}.MAXOBJECT')
            totals["tables"] = int(cur.fetchone()[0] or 0)

            cur.execute(f'SELECT COUNT(SEQUENCENAME) FROM {sch}.MAXSEQUENCE')
            totals["sequences"] = int(cur.fetchone()[0] or 0)

            cur.execute(f'SELECT COUNT(VIEWNAME) FROM {sch}.MAXVIEW')
            totals["views"] = int(cur.fetchone()[0] or 0)

            cur.execute(f'SELECT COUNT(NAME) FROM {sch}.MAXSYSINDEXES')
            totals["indexes"] = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM ALL_TRIGGERS at
                JOIN {sch}.MAXOBJECT mo ON mo.OBJECTNAME = at.TABLE_NAME
                WHERE at.OWNER = :owner
                """,
                owner=sch
            )
            totals["triggers"] = int(cur.fetchone()[0] or 0)
        else:
            cur.execute("SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :owner", owner=sch)
            totals["tables"] = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT COUNT(*) FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = :owner", owner=sch)
            totals["sequences"] = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT COUNT(*) FROM ALL_INDEXES WHERE OWNER = :owner", owner=sch)
            totals["indexes"] = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT COUNT(*) FROM ALL_TRIGGERS WHERE OWNER = :owner", owner=sch)
            totals["triggers"] = int(cur.fetchone()[0] or 0)

            cur.execute("SELECT COUNT(*) FROM ALL_VIEWS WHERE OWNER = :owner", owner=sch)
            totals["views"] = int(cur.fetchone()[0] or 0)

        return totals
    finally:
        cur.close()
        conn.close()


def _get_mssql_totals(schema: str, prefer_maximo_metadata: bool) -> Dict[str, int]:
    creds = load_credentials("sql", is_target=False) or load_credentials("sqlserver", is_target=False)
    if not creds:
        raise HTTPException(status_code=400, detail="SQL Server source credentials not found.")

    conn = _mssql_connect(creds)
    cur = conn.cursor()
    totals = {"tables": 0, "sequences": 0, "triggers": 0, "indexes": 0, "views": 0}
    sch = _normalize_schema(schema)

    try:
        if prefer_maximo_metadata:
            q_schema = f"[{sch}]"
            cur.execute(f"SELECT COUNT(OBJECTNAME) FROM {q_schema}.[MAXOBJECT]")
            totals["tables"] = int(cur.fetchone()[0] or 0)

            cur.execute(f"SELECT COUNT(SEQUENCENAME) FROM {q_schema}.[MAXSEQUENCE]")
            totals["sequences"] = int(cur.fetchone()[0] or 0)

            cur.execute(f"SELECT COUNT(VIEWNAME) FROM {q_schema}.[MAXVIEW]")
            totals["views"] = int(cur.fetchone()[0] or 0)

            cur.execute(f"SELECT COUNT(NAME) FROM {q_schema}.[MAXSYSINDEXES]")
            totals["indexes"] = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM sys.triggers t
                JOIN sys.tables tb ON t.parent_id = tb.object_id
                JOIN {q_schema}.[MAXOBJECT] mo ON mo.OBJECTNAME = tb.name
                JOIN sys.schemas s ON tb.schema_id = s.schema_id
                WHERE s.name = ?
                """,
                (sch,)
            )
            totals["triggers"] = int(cur.fetchone()[0] or 0)
        else:
            cur.execute("""
                SELECT COUNT(*) FROM sys.tables tb
                JOIN sys.schemas s ON tb.schema_id = s.schema_id
                WHERE s.name = ?
            """, (sch,))
            totals["tables"] = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COUNT(*) FROM sys.sequences sq
                JOIN sys.schemas s ON sq.schema_id = s.schema_id
                WHERE s.name = ?
            """, (sch,))
            totals["sequences"] = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COUNT(*) FROM sys.indexes i
                JOIN sys.objects o ON i.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = ? AND o.type = 'U' AND i.index_id > 0
            """, (sch,))
            totals["indexes"] = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COUNT(*) FROM sys.triggers t
                JOIN sys.tables tb ON t.parent_id = tb.object_id
                JOIN sys.schemas s ON tb.schema_id = s.schema_id
                WHERE s.name = ?
            """, (sch,))
            totals["triggers"] = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COUNT(*) FROM sys.views v
                JOIN sys.schemas s ON v.schema_id = s.schema_id
                WHERE s.name = ?
            """, (sch,))
            totals["views"] = int(cur.fetchone()[0] or 0)

        return totals
    finally:
        cur.close()
        conn.close()


@router.get("", response_model=TotalsResponse)
async def get_total_source_objects(
    source_type: str = Query(..., description="oracle or sql/sqlserver"),
    schema: str = Query(..., description="Schema/owner name to count objects in"),
    prefer_maximo_metadata: bool = Query(
        True,
        description="If true, use MAXOBJECT/MAXSEQUENCE/MAXVIEW/MAXSYSINDEXES filters when available"
    ),
):
    st = source_type.lower()
    try:
        if st == "oracle":
            totals = _get_oracle_totals(schema, prefer_maximo_metadata)
        elif st in ("sql", "sqlserver"):
            totals = _get_mssql_totals(schema, prefer_maximo_metadata)
        else:
            raise HTTPException(
                status_code=400,
                detail="Unsupported source_type. Use 'oracle' or 'sqlserver'."
            )

        return TotalsResponse(
            source_type=st,
            schema_name=_normalize_schema(schema),
            totals=totals
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get totals")
        raise HTTPException(status_code=500, detail=str(e))
