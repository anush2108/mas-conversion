// src/components/cdbConnection.jsx
import PouchDB from 'pouchdb-browser';

const couchdbUrl = process.env.COUCHDB_URL; // ensure REACT_APP_ prefix
const dbName = "transaction";

if (!couchdbUrl) {
  throw new Error("REACT_APP_COUCHDB_URL is not defined in .env");
}

const db = new PouchDB(
  `https://admin:changeme@${couchdbUrl.replace(/\/$/, '')}/${dbName}`,
  {
    skip_setup: false
  }
);

// Optional: test connection
db.info()
  .then(info => console.log("DB info:", info))
  .catch(err => console.error("DB connection error:", err));

export default db;
