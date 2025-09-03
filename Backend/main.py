# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

# -------------------------------
# ✅ Import API Routers
# -------------------------------
from routes.test_connection import router as connection_router
from routes.credentials import router as credentials_router
from routes.ddl import router as ddl_router
from routes.migrate_oracle import router as migrate_oracle_router
from routes.migrate_sql import router as migrate_sql_router
from routes.override import router as override_router
from routes.sequences import router as sequences_router
from routes.triggers import router as triggers_router
from routes.validate_data import router as validation_router
from routes.schema_list import router as schema_list_router
from routes.table_list import router as table_list_router
from routes.schema_migration_stream import router as migration_stream_router
from routes.full_schema_migration import router as full_migration_router
from routes import indexes
from routes import views
from routes.auth import router as auth_router
from routes.complex_compute import router as compute_router
from routes.total_source_object import router as total_source_object_router
from routes.embedded_sql import router as embedded_sql_router

# ✅ Newly added routes
from routes import migration_status     # /migration-status/{transaction_id}
from routes import current_migration    # /current-migration

# -------------------------------
# ✅ FASTAPI App Instance
# -------------------------------
app = FastAPI(title="DB Migration API")

# -------------------------------
# ✅ CORS Config
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Change to your frontend domain if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# ✅ No-Cache Headers Middleware
# -------------------------------
class NoCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response: Response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

app.add_middleware(NoCacheMiddleware)

# -------------------------------
# ✅ Include All Routers
# -------------------------------
app.include_router(connection_router)
app.include_router(credentials_router)
app.include_router(ddl_router)
app.include_router(migrate_oracle_router)
app.include_router(migrate_sql_router)
app.include_router(override_router)
app.include_router(sequences_router)
app.include_router(triggers_router)
app.include_router(validation_router)
app.include_router(schema_list_router)
app.include_router(table_list_router)
app.include_router(migration_stream_router)
app.include_router(indexes.router)
app.include_router(views.router)
app.include_router(auth_router)
app.include_router(full_migration_router)
app.include_router(compute_router)
app.include_router(total_source_object_router)


app.include_router(current_migration.router)   # Now /current-migration works
app.include_router(migration_status.router)    # For migration status by transaction ID
app.include_router(embedded_sql_router)
