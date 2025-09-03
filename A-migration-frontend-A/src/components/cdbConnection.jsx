// src/components/cdbConnection.jsx
import PouchDB from 'pouchdb-browser';

// Hardcoded CouchDB URL and credentials for testing
const couchdbUrl = "https://couchdb-route-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com";
const username = "admin";
const password = "changeme";
const dbName = "transaction";

// Create PouchDB instance with Basic Auth
const db = new PouchDB(`${couchdbUrl.replace(/\/$/, '')}/${dbName}`, {
  skip_setup: false,
  fetch: (url, opts) => {
    opts.headers.set('Authorization', 'Basic ' + btoa(`${username}:${password}`));
    return PouchDB.fetch(url, opts);
  }
});

// Optional: test connection
db.info()
  .then(info => console.log("DB info:", info))
  .catch(err => console.error("DB connection error:", err));

export default db;
