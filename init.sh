#!/bin/sh
set -e

echo "⏳ Waiting for CouchDB to start..."

# Wait until CouchDB is fully ready
until curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} http://couchdb:5984/_up | grep -q '"status":"ok"'; do
  echo "⏳ CouchDB not ready yet... retrying in 3s"
  sleep 3
done

echo "🚀 CouchDB is up, creating system databases..."

# Create system DBs (idempotent: succeeds even if they already exist)
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_users          || true
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_replicator     || true
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_global_changes || true
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/transaction     || true

echo "✅ Databases initialized."

echo "⚙️ Enabling CORS..."

# Enable CORS so frontend can access CouchDB
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_node/_local/_config/httpd/enable_cors -d '"true"'
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_node/_local/_config/cors/origins -d '"*"'
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_node/_local/_config/cors/methods -d '"GET, PUT, POST, HEAD, DELETE"'
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_node/_local/_config/cors/headers -d '"accept, authorization, content-type, origin, referer, x-csrf-token"'
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/_node/_local/_config/cors/credentials -d '"true"'

echo "✅ CORS enabled."

echo "🔓 Removing security on 'transaction' DB (public access)..."

# Remove all security restrictions on 'transaction'
curl -s -u ${COUCHDB_USER}:${COUCHDB_PASSWORD} -X PUT http://couchdb:5984/transaction/_security \
  -H "Content-Type: application/json" \
  -d '{
    "admins": { "names": [], "roles": [] },
    "members": { "names": [], "roles": [] }
  }'

echo "✅ 'transaction' is now open to everyone (zero privileges)."
echo "🎉 CouchDB initialization complete!"
