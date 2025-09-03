
# === services/auth_service.py ===

import httpx
from fastapi import HTTPException, Request
from pydantic import BaseModel
import os
import traceback
from utils.couchdb import get_user_by_email, update_user_password, get_email_from_auth_session

COUCHDB_URL = os.getenv("COUCHDB_URL", "http://couchdb-route-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com")
COUCHDB_ADMIN = os.getenv("COUCHDB_ADMIN", "admin")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "changeme")

class UserSignup(BaseModel):
    email: str
    password: str

class UserLogin(BaseModel):
    email: str
    password: str

class PasswordChangeRequest(BaseModel):
    current_password: str = None  
    new_password: str

async def create_user(user: UserSignup):
    print(f"🚀 Creating user: {user.email}")
    
    try:
        if not user.email or not user.password:
            print("❌ Missing email or password")
            raise HTTPException(status_code=400, detail="Email and password are required")
        
        # Basic email validation
        if "@" not in user.email or len(user.email) < 3:
            print("❌ Invalid email format")
            raise HTTPException(status_code=400, detail="Invalid email format")
        
        if len(user.password) < 6:
            print("❌ Password too short")
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
        
        user_id = f"org.couchdb.user:{user.email}"
        print(f"📝 User ID: {user_id}")
        
        user_doc = {
            "_id": user_id,
            "name": user.email,
            "password": user.password,
            "type": "user",
            "roles": []
        }
        
        async with httpx.AsyncClient(auth=(COUCHDB_ADMIN, COUCHDB_PASSWORD)) as client:
            print(f"🔍 Checking if user exists")
            
            # Check if user already exists
            try:
                check = await client.get(f"{COUCHDB_URL}/_users/{user_id}")
                
                if check.status_code == 200:
                    print("❌ User already exists")
                    raise HTTPException(status_code=400, detail="User already exists")
                    
            except httpx.RequestError as e:
                print(f"❌ Network error during user check: {e}")
                raise HTTPException(status_code=500, detail="Database connection error")
            
            # Create the user
            print(f"🛠️ Creating user")
            try:
                res = await client.put(f"{COUCHDB_URL}/_users/{user_id}", json=user_doc)
                print(f"📤 Create user response: {res.status_code}")
                
                if res.status_code not in [200, 201]:
                    print(f"❌ Failed to create user: {res.status_code} - {res.text}")
                    
                    try:
                        error_data = res.json()
                        error_detail = error_data.get("reason", f"HTTP {res.status_code}")
                    except:
                        error_detail = f"HTTP {res.status_code}: {res.text}"
                    
                    raise HTTPException(
                        status_code=500, 
                        detail=f"Failed to create user: {error_detail}"
                    )
                
                print("✅ User created successfully")
                return {"message": "User created successfully"}
                
            except httpx.RequestError as e:
                print(f"❌ Network error during user creation: {e}")
                raise HTTPException(status_code=500, detail="Database connection error")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unexpected error in create_user: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

async def get_account_details(request: Request):
    """Get account details for the authenticated user"""
    cookie_value = request.cookies.get("AuthSession")
    if not cookie_value:
        raise HTTPException(status_code=401, detail="Not authenticated")

    email = await get_email_from_auth_session(cookie_value)
    if not email:
        raise HTTPException(status_code=401, detail="Invalid session")

    return {"email": email}

async def change_password(email: str, new_password: str, current_password: str = None):
    """Change user password with proper validation"""
    try:
        # Validate new password
        if not new_password:
            raise HTTPException(status_code=400, detail="New password cannot be empty")
        
        if len(new_password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters long")
        
        # Optional: Verify current password if provided
        if current_password:
            # You can add current password verification here if needed
            print(f"🔍 Current password verification requested for {email}")
            
            # Verify current password by attempting to authenticate
            async with httpx.AsyncClient() as client:
                verify_response = await client.post(
                    f"{COUCHDB_URL}/_session",
                    data=f"name={email}&password={current_password}",
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
                
                if verify_response.status_code != 200:
                    raise HTTPException(status_code=400, detail="Current password is incorrect")
                
                verify_data = verify_response.json()
                if not verify_data.get("ok") or verify_data.get("name") != email:
                    raise HTTPException(status_code=400, detail="Current password is incorrect")
        
        # Update the password
        print(f"🔐 Changing password for user: {email}")
        success = await update_user_password(email, new_password)
        
        if success:
            print(f"✅ Password changed successfully for {email}")
            return {"message": "Password updated successfully", "success": True}
        else:
            print(f"❌ Password change failed for {email}")
            raise HTTPException(status_code=500, detail="Failed to update password")
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unexpected error in change_password: {e}")
        print(f"📋 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Password change failed: {str(e)}")
