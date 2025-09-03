import React, { useState, useEffect } from "react";
import axios from "axios";
import {
  Button,
  TextInput,
  Modal,
  Loading,
  StructuredListWrapper,
  StructuredListHead,
  StructuredListRow,
  StructuredListBody,
  StructuredListCell,
} from "@carbon/react";
import { Download, Copy } from "@carbon/icons-react";
import Header from "../components/Header";
import Sidebar from "../components/Sidebar";
import Footer from "../components/Footer";
import "../styles/EmbeddedSQLMigrationPage.css";
const API_BASE = "http://localhost:8000";
const EmbeddedSQLMigrationPage = () => {
  const [allowedTableColumns, setAllowedTableColumns] = useState([]);
  const [selectedPair, setSelectedPair] = useState(null);
  const [functionName, setFunctionName] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searching, setSearching] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [convertedSQL, setConvertedSQL] = useState("");
  useEffect(() => {
    setLoading(true);
    axios
      .get(`${API_BASE}/embedded_sqltable`)
      .then((res) => setAllowedTableColumns(res.data))
      .catch(() => alert("Failed to load allowed tables/columns."))
      .finally(() => setLoading(false));
  }, []);
  const handleSearch = async () => {
    if (!selectedPair || !functionName.trim()) {
      alert("Please select a table and enter a keyword.");
      return;
    }
    setSearching(true);
    try {
      const res = await axios.get(`${API_BASE}/embedded_sqltable/rows`, {
        params: { table: selectedPair.table, function_name: functionName },
      });
      setResults(Array.isArray(res.data) ? res.data : []);
    } catch {
      alert("Error fetching rows!");
    } finally {
      setSearching(false);
    }
  };
  const handleConvert = async (row) => {
    try {
      const colName = row.COLUMN_NAME;
      const value = row[colName];
      const res = await axios.post(`${API_BASE}/embedded_sqltable/convert`, {
        table: row.TABLE_NAME,
        column: colName,
        value,
      });
      if (res.data.error) {
        alert("Conversion error: " + res.data.error);
        return;
      }
      setConvertedSQL(res.data.converted);
      setModalOpen(true);
    } catch {
      alert("Error converting SQL to DB2!");
    }
  };
  const copyToClipboard = () => {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(convertedSQL).then(
        () => alert("Copied to clipboard!"),
        () => alert("Failed to copy to clipboard.")
      );
    }
  };
  const downloadSQL = () => {
    const blob = new Blob([convertedSQL], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "converted_query.sql";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };
  return (
    <div className="embedded-sql-page">
      <Header />
      <div className="embedded-sql-body">
        <aside className="embedded-sql-sidebar">
          <Sidebar />
        </aside>
        <main className="embedded-sql-main">
          <h2 className="page-title">Embedded SQL Conversion</h2>
          <div className="split-area">
            {/* Left panel: Allowed Tables & Columns */}
            <div className="tables-panel">
              <h3>Allowed Tables & Columns</h3>
              {loading ? (
                <div className="spinner-container">
                  <Loading description="Loading tables..." />
                </div>
              ) : (
                <div className="scroll-panel">
                  <ul className="tc-scroll-list">
                    {allowedTableColumns.length === 0 && (
                      <li className="empty-li">No allowed table-column pairs found.</li>
                    )}
                    {allowedTableColumns.map((tc, idx) => (
                      <li
                        key={idx}
                        className={
                          selectedPair &&
                          selectedPair.table === tc.table &&
                          selectedPair.column === tc.column
                            ? "tc-item selected"
                            : "tc-item"
                        }
                        onClick={() => {
                          setSelectedPair(tc);
                          setResults([]);
                          setFunctionName("");
                        }}
                        tabIndex={0}
                        aria-label={`Select ${tc.table} [${tc.column}]`}
                      >
                        <span className="tc-table">{tc.table}</span>{" "}
                        <span className="tc-column">[{tc.column}]</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
            {/* Right panel: Search and results */}
            <div className="search-panel">
              {selectedPair ? (
                <>
                  <div className="search-header">
                    <label>
                      Searching in:{" "}
                      <span className="tc-highlight">
                        {selectedPair.table} [{selectedPair.column}]
                      </span>
                    </label>
                  </div>
                  <div className="input-row">
                    <TextInput
                      id="functionNameRight"
                      value={functionName}
                      onChange={(e) => setFunctionName(e.target.value)}
                      placeholder="e.g. SELECT, JOIN, WHERE"
                      labelText=""
                      style={{ flexGrow: 1 }}
                    />
                    <Button
                      kind="primary"
                      size="sm"
                      onClick={handleSearch}
                      disabled={searching}
                      style={{ marginLeft: "8px" }}
                    >
                      {searching ? "Searching..." : "Search"}
                    </Button>
                  </div>
                  <div className="results-section-right">
                    {results.length > 0 ? (
                      <div className="table-container">
                        <StructuredListWrapper>
<StructuredListHead>
  <StructuredListRow head>
    <StructuredListCell head>
      IDENTIFIER_VALUE
      {selectedPair && selectedPair.identifier_column_name
        ? `(${selectedPair.identifier_column_name})`
        : ""}
    </StructuredListCell>
    <StructuredListCell head>VALUE</StructuredListCell>
    <StructuredListCell head>Action</StructuredListCell>
  </StructuredListRow>
</StructuredListHead>
                          <StructuredListBody>
                            {results.map((row, idx) => {
                              const colName = row.COLUMN_NAME;
                              const value = row[colName];
                              const idValue = row.IDENTIFIER_VALUE || "?";
                              return (
                                <StructuredListRow key={idx}>
                                  <StructuredListCell
                                    style={{
                                      whiteSpace: "pre-wrap",
                                      maxWidth: 200,
                                      overflowWrap: "break-word",
                                    }}
                                  >
                                    {String(idValue)}
                                  </StructuredListCell>
                                  <StructuredListCell
                                    style={{
                                      whiteSpace: "pre-wrap",
                                      maxWidth: 400,
                                      overflowWrap: "break-word",
                                    }}
                                  >
                                    {String(value)}
                                  </StructuredListCell>
                                  <StructuredListCell>
                                    <Button size="sm" onClick={() => handleConvert(row)}>
                                      Convert to DB2
                                    </Button>
                                  </StructuredListCell>
                                </StructuredListRow>
                              );
                            })}
                          </StructuredListBody>
                        </StructuredListWrapper>
                      </div>
                    ) : (
                      <p>No results found.</p>
                    )}
                  </div>
                </>
              ) : (
                <div className="search-panel-placeholder">
                  <em>Select a table-column from the left to begin searching.</em>
                </div>
              )}
            </div>
          </div>
        </main>
      </div>
      <Footer />
      {/* Modal: DB2 SQL output, Carbon UI */}
      <Modal
        open={modalOpen}
        onRequestClose={() => setModalOpen(false)}
        modalHeading="DB2 Converted SQL"
        primaryButtonText="Close"
        onRequestSubmit={() => setModalOpen(false)}
        size="lg"
      >
        <pre
          style={{
            maxHeight: "60vh",
            overflowY: "auto",
            backgroundColor: "#f4f4f4",
            padding: "1rem",
            fontFamily: "monospace",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
            borderRadius: 4,
            border: "1px solid #8c8c8c",
          }}
        >
          {convertedSQL}
        </pre>
        <div style={{ marginTop: "1rem", display: "flex", gap: "1rem" }}>
          <Button renderIcon={Copy} onClick={copyToClipboard} size="sm">
            Copy to Clipboard
          </Button>
          <Button renderIcon={Download} onClick={downloadSQL} kind="secondary" size="sm">
            Download SQL
          </Button>
        </div>
      </Modal>
    </div>
  );
};
export default EmbeddedSQLMigrationPage;
