// src/components/NavTabs.jsx
import React from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../styles/NavTabs.css";

const tabList = [
  { label: "Schema", path: "/full-schema-migration" },
  { label: "Tables", path: "/table-migration" }, // ✅ Updated here
  { label: "Sequences", path: "/sequences" },
  { label: "Triggers", path: "/trigger-migration" },
  { label: "Views", path: "/views" },
  { label: "Indexes", path: "/index-migration" },
];

const NavTabs = ({ state }) => {
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <div className="nav-tabs">
      {tabList.map((tab) => (
        <div
          key={tab.path}
          className={`nav-tab ${location.pathname === tab.path ? "active" : ""}`}
          onClick={() => navigate(tab.path, { state })}
        >
          {tab.label}
        </div>
      ))}
    </div>
  );
};

export default NavTabs;
