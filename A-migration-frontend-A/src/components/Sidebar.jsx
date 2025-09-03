import React, { useContext, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { Home, Document, Information } from "@carbon/icons-react";
import { AuthContext } from "../context/AuthContext";
import { useMigrationContext } from "../context/MigrationContext";
import { Repeat } from "@carbon/icons-react";
import "../styles/Sidebar.css";
import axios from "axios";
import { InlineNotification } from "@carbon/react";
import ViewDDL from "./ViewDDL"; // Make sure the path is correct

const Sidebar = () => {
  const { isLoggedIn } = useContext(AuthContext);
  const [open, setOpen] = useState(false);
  const location = useLocation();
  const { setModalOpen, setCurrentMigration } = useMigrationContext();
  const [notif, setNotif] = useState(null);
  const [viewDDLModalOpen, setViewDDLModalOpen] = useState(false); // State to manage ViewDDL dialog

  const toggleSidebar = () => setOpen(!open);

  const handleMigrationStatusClick = async (e) => {
    e.preventDefault();
    try {
      const res = await axios.get("http://localhost:8000/current-migration");
      setCurrentMigration(res.data);
      setModalOpen(true);
      setNotif(null);
    } catch (err) {
      const message =
        err.response?.data?.detail || "No migration is currently running.";
      setNotif({ kind: "error", title: "No Migration", subtitle: message });
      setModalOpen(false);
      setCurrentMigration(null);
    }
  };

  // Handler for View DDL click
  const handleViewDDLClick = (e) => {
    e.preventDefault();
    setViewDDLModalOpen(true);
  };

  return (
    <nav className={`sidebar ${open ? "open" : ""}`} aria-label="Main navigation">
      <div className="sidebar-header">
        <button
          className="menu-toggle"
          onClick={toggleSidebar}
          aria-expanded={open}
          aria-label={open ? "Close menu" : "Open menu"}
          type="button"
        >
          ☰
        </button>
      </div>

      <div className="sidebar-menu">
        <Link to="/" className={`sidebar-link ${location.pathname === "/" ? "active" : ""}`}>
          <Home />
          <span>Home</span>
        </Link>

        <Link to="/faqs" className={`sidebar-link ${location.pathname === "/faqs" ? "active" : ""}`}>
          <Information />
          <span>FAQs</span>
        </Link>

        {isLoggedIn && (
          <>
            {/* Remove Dashboard Link */}
            {/* <Link to="/dashboard" className={`sidebar-link ${location.pathname === "/dashboard" ? "active" : ""}`}>
              <Dashboard />
              <span>Dashboard</span>
            </Link> */}

            {/* Add View DDL option */}
            <a
              href="/view-ddl"
              onClick={handleViewDDLClick}
              className="sidebar-link"
            >
              <Document />
              <span>View DDL</span>
            </a>

            <a
  href="/migration-status"
  onClick={handleMigrationStatusClick}
  className={`sidebar-link ${location.pathname === "/migration-status" ? "active" : ""}`}
>
  <Repeat /> {/* Replace with your chosen icon */}
  <span>Conversion Status</span>
</a>
          </>
        )}
      </div>

      {/* Inline Notification */}
      {notif && (
        <div style={{ position: "fixed", bottom: "2rem", left: "2rem", zIndex: 9999 }}>
          <InlineNotification {...notif} onClose={() => setNotif(null)} />
        </div>
      )}

      {/* Render the ViewDDL modal */}
      <ViewDDL open={viewDDLModalOpen} onClose={() => setViewDDLModalOpen(false)} />
    </nav>
  );
};

export default Sidebar;
