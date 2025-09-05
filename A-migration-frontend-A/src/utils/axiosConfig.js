import axios from "axios";

const api = axios.create({
  baseURL: "https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com", 
  timeout: 30000,
  headers: {
    "Content-Type": "application/json",
  },
});

export default api;
