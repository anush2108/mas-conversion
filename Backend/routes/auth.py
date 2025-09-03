
# === routes/auth.py ===

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from services.auth_service import (
    create_user, UserSignup,
    get_account_details, change_password
)
from utils.couchdb import get_user_by_email, get_session_info
import os
from dotenv import load_dotenv
import httpx

load_dotenv()
COUCHDB_URL = os.getenv("COUCHDB_URL")
COUCHDB_ADMIN = os.getenv("COUCHDB_ADMIN", "admin")

print("Auth router loaded")
router = APIRouter(prefix="/auth", tags=["Auth"])

@router.post("/signup")
async def signup(user: UserSignup):
    result = await create_user(user)
    return JSONResponse(status_code=201, content=result)

@router.post("/login")
async def login_proxy(request: Request):
    body = await request.json()
    email = body.get("email")
    password = body.get("password")

    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password are required")

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{COUCHDB_URL}/_session",
            data=f"name={email}&password={password}",
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )

    if response.status_code == 200:
        response_data = response.json()
        if not response_data.get("ok") or not response_data.get("name"):
            raise HTTPException(status_code=401, detail="Invalid email or password")

        logged_in_user = response_data.get("name")

        # Extract AuthSession from Set-Cookie header
        set_cookie_header = response.headers.get("set-cookie")
        if not set_cookie_header or "AuthSession=" not in set_cookie_header:
            raise HTTPException(status_code=500, detail="Authentication failed - no session cookie received")

        auth_session_value = None
        cookies = set_cookie_header.split(", ")
        for cookie in cookies:
            if "AuthSession=" in cookie:
                auth_session_value = cookie.split("AuthSession=")[1].split(";")[0]
                break

        if not auth_session_value:
            raise HTTPException(status_code=500, detail="Failed to extract session cookie")

        fastapi_response = JSONResponse(content={
            "ok": True,
            "message": "Login successful",
            "name": logged_in_user
        })

        fastapi_response.set_cookie(
            key="AuthSession",
            value=auth_session_value,
            httponly=True,
            samesite="Strict",
            secure=False,
            max_age=86400,
            path="/"
        )

        return fastapi_response

    else:
        try:
            error_data = response.json()
            error_message = error_data.get("reason", "Login failed")
        except:
            error_message = "Login failed"
        raise HTTPException(status_code=401, detail=error_message)

@router.get("/account")
async def get_account(request: Request):
    """Get current user account information"""
    cookie_value = request.cookies.get("AuthSession")

    if not cookie_value:
        raise HTTPException(status_code=401, detail="Missing AuthSession cookie")

    session_info = await get_session_info(cookie_value)

    if not session_info or not session_info.get("name"):
        raise HTTPException(status_code=401, detail="Invalid session or cookie")

    username = session_info["name"]
    return {"email": username}

@router.post("/account/change-password")
async def change_password_api(request: Request):
    """Change user password endpoint"""
    # Verify authentication
    cookie_value = request.cookies.get("AuthSession")
    if not cookie_value:
        raise HTTPException(status_code=401, detail="Missing authentication cookie")

    session_info = await get_session_info(cookie_value)
    if not session_info or not session_info.get("name"):
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    username = session_info["name"]

    # Get request body
    body = await request.json()
    new_password = body.get("new_password")
    current_password = body.get("current_password")  # Optional

    if not new_password:
        raise HTTPException(status_code=400, detail="New password is required")

    # Change the password
    try:
        result = await change_password(username, new_password, current_password)
        
        # Create response and clear session cookie to force re-login
        response = JSONResponse(content={
            "message": "Password updated successfully. Please log in again.",
            "success": True
        })
        
        # Clear the session cookie to force re-authentication
        response.delete_cookie("AuthSession", path="/")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Password change endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Failed to change password")

@router.post("/logout")
async def logout(request: Request):
    session_cookie = request.cookies.get("AuthSession")

    if session_cookie:
        async with httpx.AsyncClient() as client:
            await client.delete(
                f"{COUCHDB_URL}/_session",
                headers={"Cookie": f"AuthSession={session_cookie}"}
            )

    response = JSONResponse(content={"message": "Logged out"})
    response.delete_cookie("AuthSession", path="/")
    return response




@router.get("/ping")
async def ping():
    return {"msg": "pong"}

@router.get("/debug/cookie")
async def debug_cookie(request: Request):
    return {"cookies": dict(request.cookies)}
