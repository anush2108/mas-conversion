import axios from "axios";

const api = axios.create({
  baseURL: "https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com", 
  timeout: 30000,
  headers: {
    "Content-Type": "application/json",
  },
});

export default api;
