from fastapi import APIRouter, Body, Query
from sse_starlette.sse import EventSourceResponse

from services.sequence_oracle_service import (
    convert_sequences_from_oracle,
    list_sequences_from_oracle,
)
from services.sequence_sql_service import (
    convert_sequences_from_mssql,
    list_sequences_from_mssql,
)
from utils.credentials_store import load_credentials, get_target_credentials

router = APIRouter(prefix="/sequences")


def _msg(text: str) -> str:
    return f"data: {text.strip()}\n\n"


@router.get("/oracle/{schema}/list")
def list_oracle_sequences(schema: str):
    try:
        oracle_creds = load_credentials("oracle")
        sequences = list_sequences_from_oracle(oracle_creds, schema)
        return {"sequences": sequences}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/sql/{schema}/list")
def list_sql_sequences(schema: str):
    try:
        sql_creds = load_credentials("sql")
        sequences = list_sequences_from_mssql(sql_creds, schema)
        return {"sequences": sequences}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# @router.get("/migrate/stream")
# async def migrate_sequences_stream(
#     source_type: str = Query(..., description="Source DB type"),
#     schema: str = Query(..., description="Schema to migrate"),
#     transaction_id: str = Query(None, description="Transaction ID"),
# ):
#     async def generator():
#         try:
#             yield _msg("🔢 Starting sequences migration...")

#             source_creds = load_credentials(source_type)
#             target_creds = get_target_credentials()

#             if source_type.lower() == "oracle":
#                 sequences = convert_sequences_from_oracle(source_creds, target_creds, schema, transaction_id)
#             else:
#                 sequences = convert_sequences_from_mssql(source_creds, target_creds, schema, transaction_id)

#             if not sequences:
#                 yield _msg("⚠️ No sequences found.")
#                 return

#             for seq in sequences:
#                 name = seq.get("sequence", "<unknown>")
#                 if seq.get("created_in_db2") or seq.get("created_in_db"):
#                     yield _msg(f"✅ Sequence '{name}' created successfully.")
#                 elif seq.get("skipped_existing"):
#                     yield _msg(f"⚠️ Sequence '{name}' already exists — skipped.")
#                 else:
#                     yield _msg(f"❌ Sequence '{name}' failed: {seq.get('error', 'Unknown error')}")

#             yield _msg(f"🎉 Sequence migration completed: {len(sequences)} total.")

#         except Exception as e:
#             yield _msg(f"❌ Migration stream error: {str(e)}")

#     return EventSourceResponse(generator())

from fastapi import APIRouter, Query
from sse_starlette.sse import EventSourceResponse
import json


def _msg(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

@router.get("/migrate/stream")
async def migrate_sequences_stream(
    source_type: str = Query(..., description="Source DB type"),
    schema: str = Query(..., description="Schema to migrate"),
    transaction_id: str = Query(..., description="Transaction ID for migration tracking"),  # mandatory here
):
    from utils.credentials_store import load_credentials, get_target_credentials
    from services.sequence_oracle_service import convert_sequences_from_oracle
    from services.sequence_sql_service import convert_sequences_from_mssql

    # Load creds
    source_creds = load_credentials(source_type)
    target_creds = get_target_credentials()

    async def generator():
        try:
            yield _msg({"logs": [{"type": "info", "message": f"🔢 Starting sequences migration for schema '{schema}' from source '{source_type}'."}], "progress": {"completed": 0, "total": 0}})

            if source_type.lower() == "oracle":
                sequences = convert_sequences_from_oracle(source_creds, target_creds, schema, transaction_id)
            else:
                sequences = convert_sequences_from_mssql(source_creds, target_creds, schema, transaction_id)

            total = len(sequences)
            completed = 0

            if total == 0:
                yield _msg({"logs": [{"type": "warning", "message": f"⚠️ No sequences found to migrate in schema '{schema}'."}], "progress": {"completed": 0, "total": 0}})
                return

            for seq in sequences:
                completed += 1
                name = seq.get("sequence", "<unknown>")
                if seq.get("created_in_db2") or seq.get("created_in_db"):
                    logs = [{"type": "success", "message": f"✅ Sequence '{name}' created successfully."}]
                elif seq.get("skipped_existing") or seq.get("skipped"):
                    logs = [{"type": "info", "message": f"⚠️ Sequence '{name}' already exists — skipped."}]
                else:
                    error_msg = seq.get("error", "Unknown error")
                    logs = [{"type": "error", "message": f"❌ Failed to create sequence '{name}': {error_msg}"}]

                progress = {"completed": completed, "total": total}

                # Yield structured log + progress info as SSE
                yield _msg({"logs": logs, "progress": progress})

            yield _msg({"logs": [{"type": "info", "message": f"🎉 Sequence migration completed: {total} total."}], "progress": {"completed": total, "total": total}})

        except Exception as e:
            yield _msg({"logs": [{"type": "error", "message": f"❌ Migration stream error: {str(e)}"}], "progress": {}})

    return EventSourceResponse(generator())
