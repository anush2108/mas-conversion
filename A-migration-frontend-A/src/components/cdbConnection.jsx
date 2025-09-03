// cdbConnection.jsx
import PouchDB from 'pouchdb-browser';

const couchdbUrl = process.env.COUCHDB_URL;
const dbName = "transaction";

const db = new PouchDB(`admin:changeme@${couchdbUrl}/${dbName}`, {
  skip_setup: false,
});

export default db;
