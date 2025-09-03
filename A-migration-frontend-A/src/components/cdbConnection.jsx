// cdbConnection.jsx
import PouchDB from 'pouchdb-browser';

const couchdbUrl = process.env.REACT_APP_COUCHDB_URL;
const dbName = "transaction";

const db = new PouchDB(`${couchdbUrl.replace(/\/$/, '')}/${dbName}`, {
  skip_setup: false,
});

export default db;
