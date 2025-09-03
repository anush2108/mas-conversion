
# routes/credentials.py


from fastapi import APIRouter
from utils.credentials_store import get_source_credentials, get_target_credentials

router = APIRouter()

@router.get("/get-source-credentials/{source_type}")
def get_source_creds(source_type: str):
    try:
        return {"status": "success", "credentials": get_source_credentials(source_type)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/get-target-credentials")
def get_target_creds():
    try:
        return {"status": "success", "credentials": get_target_credentials()}
    except Exception as e:
        return {"status": "error", "message": str(e)}
