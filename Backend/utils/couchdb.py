# === utils/couchdb.py ===

import httpx
import os
from dotenv import load_dotenv

load_dotenv()

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://localhost:5984")
COUCHDB_ADMIN = os.getenv("COUCHDB_ADMIN", "admin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "admin")

async def get_user_by_email(email: str):
    """Get user document by email"""
    user_id = f"org.couchdb.user:{email}"
    async with httpx.AsyncClient(auth=(COUCHDB_ADMIN, COUCHDB_PASSWORD)) as client:
        try:
            res = await client.get(f"{COUCHDB_URL}/_users/{user_id}")
            if res.status_code == 200:
                return res.json()
            return None
        except Exception as e:
            print(f"❌ Error fetching user: {e}")
            return None

async def update_user_password(email: str, new_password: str):
    """Update user password in CouchDB"""
    user_id = f"org.couchdb.user:{email}"
    async with httpx.AsyncClient(auth=(COUCHDB_ADMIN, COUCHDB_PASSWORD)) as client:
        try:
            # First get the current user document
            res = await client.get(f"{COUCHDB_URL}/_users/{user_id}")
            
            if res.status_code == 200:
                user_doc = res.json()
                
                # Update the password while preserving other fields
                user_doc["password"] = new_password
                user_doc["type"] = "user"
                if "roles" not in user_doc:
                    user_doc["roles"] = []
                
                print(f"🔧 Updating password for user: {email}")
                
                # Update the user document
                update_res = await client.put(f"{COUCHDB_URL}/_users/{user_id}", json=user_doc)
                
                if update_res.status_code in [200, 201]:
                    print(f"✅ Password updated successfully for {email}")
                    return True
                else:
                    print(f"❌ Failed to update password: {update_res.status_code} - {update_res.text}")
                    return False
            else:
                print(f"❌ User not found: {email}")
                return False

        except Exception as e:
            print(f"❌ Password update error: {e}")
            return False

async def get_session_info(cookie_value: str) -> dict:
    """Get session info from CouchDB auth session cookie"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{COUCHDB_URL}/_session",
                headers={"Cookie": f"AuthSession={cookie_value}"}
            )
            
            if response.status_code != 200:
                print(f"❌ Session validation failed: {response.status_code}")
                return None
            
            session_data = response.json()
            user_ctx = session_data.get("userCtx", {})
            username = user_ctx.get("name")
            
            if not username:
                print("❌ No username in session")
                return None
            
            return {"name": username}

    except httpx.RequestError as e:
        print(f"❌ Network error validating session: {e}")
        return None
    except Exception as e:
        print(f"❌ Session validation error: {e}")
        return None

async def get_email_from_auth_session(cookie_value: str) -> str:
    """Get email from CouchDB auth session cookie"""
    session_info = await get_session_info(cookie_value)
    return session_info.get("name") if session_info else None
