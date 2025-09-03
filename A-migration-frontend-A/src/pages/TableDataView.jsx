import React, { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import "../styles/TableDataView.css";
import Header from "../components/Header";
import Footer from "../components/Footer";

const TableDataView = () => {
  const { state } = useLocation();
  const navigate = useNavigate();

  const { sourceDbType, schema, table } = state || {};
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [migrating, setMigrating] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage, setItemsPerPage] = useState(50);

  const apiPrefix = sourceDbType === "oracle" ? "/o_schemas" : "/s_schemas";

  useEffect(() => {
    if (!schema || !table) return;

    fetch(`https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com${apiPrefix}/${schema}/${table}`)
      .then((res) => res.json())
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [schema, table, apiPrefix]);

  const exportToCSV = () => {
    if (data.length === 0) return;
    
    const csv = [
      Object.keys(data[0]).join(","),
      ...data.map((row) =>
        Object.values(row)
          .map((val) => `"${String(val).replace(/"/g, '""')}"`)
          .join(",")
      ),
    ].join("\n");

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.setAttribute("download", `${schema}_${table}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleMigrateTable = async () => {
    setMigrating(true);
    try {
      const res = await fetch(
        `https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com${apiPrefix}/${schema}/${table}/migrate`,
        {
          method: "POST",
        }
      );
      const result = await res.json();
      if (!res.ok) throw new Error(result.message || "Conversion failed");
      alert(`✅ Table converted successfully: ${result.message || "Done"}`);
    } catch (error) {
      console.error(error);
      alert(`❌ Conversion failed: ${error.message}`);
    } finally {
      setMigrating(false);
    }
  };

  const handleDelete = () => {
    if (window.confirm(`Are you sure you want to delete table ${schema}.${table}?`)) {
      // Add delete functionality here
      console.log("Delete table:", schema, table);
    }
  };

  const headers = data.length > 0 ? Object.keys(data[0]) : [];
  
  // Pagination logic
  const totalItems = data.length;
  const totalPages = Math.ceil(totalItems / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = Math.min(startIndex + itemsPerPage, totalItems);
  const currentData = data.slice(startIndex, endIndex);

  const handlePageChange = (page) => {
    setCurrentPage(page);
  };

  const handleItemsPerPageChange = (e) => {
    setItemsPerPage(parseInt(e.target.value));
    setCurrentPage(1);
  };

  if (loading) {
    return (
      <div className="table-container">
        <div className="loading-container">
          <div className="loading-spinner"></div>
          <span>Loading table data...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="table-container">
      <Header />
      {/* Header Section */}
      <div className="table-header">
        <div className="table-title">
          <h2>{schema && table ? `${schema}.${table}` : 'Table Data'}</h2>
        </div>
        <div className="table-actions">
          <button className="btn btn-back" onClick={() => navigate(-1)}>
            Back
          </button>
          <button 
            className="btn btn-secondary" 
            onClick={handleDelete}
            title="Delete table"
          >
            🗑️
          </button>
          <button 
            className="btn btn-secondary" 
            onClick={handleMigrateTable}
            disabled={migrating}
          >
            {migrating ? "Converting..." : "Convert Table"}
          </button>
          <button className="btn btn-primary" onClick={exportToCSV}>
            📥 Export to CSV
          </button>
        </div>
      </div>

      {/* Data Table */}
      {data.length === 0 ? (
        <div className="no-data">
          <p>No data found.</p>
        </div>
      ) : (
        <>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  {headers.map((header) => (
                    <th key={header}>{header.toUpperCase()}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {currentData.map((row, index) => (
                  <tr key={startIndex + index}>
                    {headers.map((header) => (
                      <td key={header}>
                        {row[header] !== null && row[header] !== undefined 
                          ? String(row[header]) 
                          : '-'
                        }
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination Footer */}
          <div className="table-footer">
            <div className="items-per-page">
              <label>Items per page:</label>
              <select 
                value={itemsPerPage} 
                onChange={handleItemsPerPageChange}
                className="items-select"
              >
                <option value={25}>25</option>
                <option value={50}>50</option>
                <option value={100}>100</option>
              </select>
            </div>
            
            <div className="pagination-info">
              <span>
                {startIndex + 1}–{endIndex} items | page {currentPage}
              </span>
              
              <div className="pagination-controls">
                <button 
                  className="pagination-btn"
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1}
                >
                  ‹
                </button>
                <span className="current-page">{currentPage}</span>
                <button 
                  className="pagination-btn"
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={currentPage === totalPages}
                >
                  ›
                </button>
              </div>
            </div>
          </div>
        </>
      )}
      <Footer />
    </div>
  );
};

export default TableDataView;
