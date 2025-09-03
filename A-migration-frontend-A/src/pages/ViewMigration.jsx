import React, { useState } from "react";
import { useLocation } from "react-router-dom";
import {
  Button,
  InlineLoading,
  ComposedModal,
  ModalHeader,
  ModalBody,
  ModalFooter,
  Checkbox,
  TextInput,
} from "@carbon/react";
import Header from "../components/Header";
import Footer from "../components/Footer";
import NavTabs from "../components/NavTabs";
import SchemaSelector from "../components/SchemaSelector";
import { useTransactionIdForSchema } from "../hooks/useTransactionIdForSchema";
import "../styles/ViewMigration.css";

const ViewMigration = () => {
  const { state } = useLocation();
  const sourceDbType = state?.sourceDbType?.toLowerCase();
  const [selectedSchema, setSelectedSchema] = useState("");
  const { transactionId, loading: txLoading } = useTransactionIdForSchema(selectedSchema);

  const [views, setViews] = useState([]);
  const [filteredViews, setFilteredViews] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedViews, setSelectedViews] = useState(new Set());
  const [loadingViews, setLoadingViews] = useState(false);
  const [showModal, setShowModal] = useState(false);
  const [migrationLogs, setMigrationLogs] = useState([]);
  const [migrating, setMigrating] = useState(false);

  const viewApiPrefix = "http://localhost:8000/views";

  const fetchViews = async (schema) => {
    setSelectedSchema(schema);
    setLoadingViews(true);
    try {
      const res = await fetch(
        `${viewApiPrefix}/list?source_type=${sourceDbType}&schema=${schema}`
      );
      const data = await res.json();
      const viewList = Array.isArray(data)
        ? data.map((v) => v.view || v.view_name || v)
        : [];
      setViews(viewList);
      setFilteredViews(viewList);
      setSelectedViews(new Set());
    } catch (err) {
      console.error(err);
      setViews([]);
      setFilteredViews([]);
    } finally {
      setLoadingViews(false);
    }
  };

  const handleMigration = async (onlySelected = false) => {
    setShowModal(true);
    setMigrationLogs([]);
    setMigrating(true);
    if (!transactionId) {
      setMigrationLogs(["❌ No Transaction ID available."]);
      setMigrating(false);
      return;
    }
    try {
      const names = onlySelected
        ? `&${Array.from(selectedViews)
            .map((v) => `view_names=${encodeURIComponent(v)}`)
            .join("&")}`
        : "";
      const url = `${viewApiPrefix}/migrate?source_type=${sourceDbType}&target=db2&schema=${selectedSchema}&transaction_id=${transactionId}${names}`;
      const response = await fetch(url, { method: "POST" });
      const result = await response.json();

      const logs = [
        `✅ Converted: ${result.total_migrated} / ${result.total_requested}`,
        ...(result.migrated || []).map((v, i) => `✅ ${i + 1}. ${v}`),
        ...(result.skipped || []).map(
          (s, i) =>
            `❌ ${result.migrated.length + i + 1}. ${s.view} - ${s.reason || "Unknown error"}`
        ),
      ];
      setMigrationLogs(logs);
      setSelectedViews(new Set());
    } catch (err) {
      setMigrationLogs([`❌ Error: ${err.message || err}`]);
    } finally {
      setMigrating(false);
    }
  };

  const toggleViewSelection = (view) => {
    setSelectedViews((prev) => {
      const updated = new Set(prev);
      updated.has(view) ? updated.delete(view) : updated.add(view);
      return updated;
    });
  };

  const handleSearch = (value) => {
    setSearchTerm(value);
    const lowerValue = value.toLowerCase();
    setFilteredViews(views.filter((v) => v.toLowerCase().includes(lowerValue)));
  };

  const handleCloseModal = () => {
    setShowModal(false);
    setMigrationLogs([]);
    setMigrating(false);
  };

  return (
    <div className="view-migration">
      <Header />
      <NavTabs state={state} />

      <main className={`main-content ${selectedSchema ? "split-view" : ""}`}>
        <div className="content-wrapper">
          <div className="schema-panel">
            <SchemaSelector
              sourceDbType={sourceDbType}
              onSchemaSelect={fetchViews}
              selectedSchema={selectedSchema}
            />
          </div>

          {selectedSchema && (
            <div className="view-panel">
              <div className="view-header">
                <h4>Views in {selectedSchema}</h4>
                <div className="button-actions">
                  <Button
                    kind="secondary"
                    size="sm"
                    onClick={() => handleMigration(false)}
                    disabled={txLoading}
                  >
                    Convert Entire Views
                  </Button>
                  <Button
                    kind="primary"
                    size="sm"
                    onClick={() => handleMigration(true)}
                    disabled={selectedViews.size === 0 || txLoading}
                  >
                    Convert Selected Views
                  </Button>
                </div>
              </div>

              <div className="count-select">
                <span>{filteredViews.length} views found</span>
                <label>
                  <input
                    type="checkbox"
                    onChange={(e) =>
                      setSelectedViews(
                        e.target.checked ? new Set(filteredViews) : new Set()
                      )
                    }
                    checked={
                      selectedViews.size === filteredViews.length &&
                      filteredViews.length > 0
                    }
                    disabled={txLoading}
                  />
                  {" "}
                  Select All
                </label>
              </div>

              <div className="search-bar-container">
                <TextInput
                  id="search-views"
                  labelText=""
                  placeholder="Search views..."
                  value={searchTerm}
                  onChange={(e) => handleSearch(e.target.value)}
                  disabled={txLoading}
                />
              </div>

              {loadingViews ? (
                <InlineLoading description="Loading views..." />
              ) : filteredViews.length > 0 ? (
                <div className="view-scroll">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th></th>
                        <th>#</th>
                        <th>View Name</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredViews.map((view, idx) => (
                        <tr key={view}>
                          <td>
                            <Checkbox
                              id={view}
                              labelText=""
                              checked={selectedViews.has(view)}
                              onChange={() => toggleViewSelection(view)}
                              disabled={txLoading}
                            />
                          </td>
                          <td>{idx + 1}</td>
                          <td>{view}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p>No views found.</p>
              )}
            </div>
          )}
        </div>
      </main>

      <ComposedModal open={showModal} onClose={handleCloseModal} size="lg">
        <ModalHeader title="View Conversion Status" closeModal={handleCloseModal} />
        <ModalBody>
          {migrationLogs.length === 0 ? (
            <InlineLoading description="Starting conversion..." />
          ) : (
            <div className="migration-logs">
              {migrationLogs.map((log, idx) => (
                <div key={idx} className="log-entry" style={{ marginBottom: "10px" }}>
                  <strong>{log}</strong>
                </div>
              ))}
            </div>
          )}
        </ModalBody>
        <ModalFooter>
          <Button kind="secondary" onClick={handleCloseModal} disabled={migrating}>
            Close
          </Button>
        </ModalFooter>
      </ComposedModal>

      <Footer />
    </div>
  );
};

export default ViewMigration;
