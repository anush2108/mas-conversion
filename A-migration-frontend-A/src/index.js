import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import "@carbon/styles/css/styles.css";
import { AuthProvider } from './context/AuthContext'; 

// Import Modal
import Modal from "react-modal";

// ✅ Set the app element once, right after import
Modal.setAppElement('#root');

// Development-only token/cookie clearing to start logged out on refresh
if (process.env.NODE_ENV === "development") {
  document.cookie = "session=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
  localStorage.removeItem("auth_token");
  sessionStorage.removeItem("auth_token");
}

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <AuthProvider>
      <App />
    </AuthProvider>
  </React.StrictMode>
);

reportWebVitals();
