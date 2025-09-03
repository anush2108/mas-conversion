// src/pages/TableMigration.jsx
import React, { useEffect, useState, useRef } from "react";
import { useLocation } from "react-router-dom";
import {
  Button,
  ComposedModal,
  ModalHeader,
  ModalBody,
  ModalFooter,
  InlineLoading,
  Tag,
  TextInput,
} from "@carbon/react";
import Header from "../components/Header";
import Footer from "../components/Footer";
import SchemaSelector from "../components/SchemaSelector";
import NavTabs from "../components/NavTabs";
import "../styles/TableMigration.css";
import MigrationStatusModal from "../components/MigrationStatusModal";
import { useTransactionIdForSchema } from "../hooks/useTransactionIdForSchema";

const TableMigration = () => {
  const { state } = useLocation();
  const sourceDbType = state?.sourceDbType?.toLowerCase();

  const [selectedSchema, setSelectedSchema] = useState("");
  const { transactionId, loading: txLoading, error: txError } =
    useTransactionIdForSchema(selectedSchema);

  const [schemaObjects, setSchemaObjects] = useState([]);
  const [selectedTables, setSelectedTables] = useState(new Set());
  const [loadingObjects, setLoadingObjects] = useState(false);
  const [showConfirmModal, setShowConfirmModal] = useState(false);
  const [showSchemaModal, setShowSchemaModal] = useState(false);
  const [showStatusModal, setShowStatusModal] = useState(false);
  const [migrating, setMigrating] = useState(false);
  const [migrationLogs, setMigrationLogs] = useState([]);
  const [migrationProgress, setMigrationProgress] = useState({ completed: 0, total: 0 });
  const [objectsProgress, setObjectsProgress] = useState([]); // NEW
  const [searchTerm, setSearchTerm] = useState("");
  const eventSourceRef = useRef(null);

  const contextType = "tables";

  // Fetch schema objects when schema changes
  useEffect(() => {
    if (selectedSchema) {
      setSchemaObjects([]);
      setSelectedTables(new Set());
      setLoadingObjects(true);
      fetchSchemaObjects(selectedSchema);
    }
  }, [selectedSchema]);

  const fetchSchemaObjects = async (schema) => {
    try {
      const res = await fetch(`http://localhost:8000/${contextType}?schema=${schema}&source=${sourceDbType}`);
      const data = await res.json();
      setSchemaObjects(Array.isArray(data) ? data : []);
    } catch (e) {
      console.error(e);
      setSchemaObjects([]);
    } finally {
      setLoadingObjects(false);
    }
  };

  const handleSchemaSelected = (schema) => {
    setSelectedSchema(schema);
  };

  const handleSelectAll = (e) => {
    if (e.target.checked) {
      setSelectedTables(new Set(filteredTables));
    } else {
      setSelectedTables(new Set());
    }
  };

  const toggleTableSelection = (table) => {
    setSelectedTables((prev) => {
      const copy = new Set(prev);
      if (copy.has(table)) copy.delete(table);
      else copy.add(table);
      return copy;
    });
  };

  const parseLogMessage = (msg) => {
    if (msg.includes("✅")) return { type: "success", message: msg };
    if (msg.includes("❌")) return { type: "error", message: msg };
    if (msg.includes("⚠️")) return { type: "warning", message: msg };
    if (msg.includes("⏰") || msg.includes("Timeout")) return { type: "error", message: msg };
    return { type: "info", message: msg };
  };

  const handleStartMigration = () => {
    if (!selectedSchema || selectedTables.size === 0 || !transactionId) return;

    setMigrating(true);
    setMigrationLogs([]);
    setMigrationProgress({ completed: 0, total: selectedTables.size });
    setObjectsProgress([]);
    setShowStatusModal(true);
    setShowConfirmModal(false);

    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    const queryParams = Array.from(selectedTables).map((t) => `tables=${encodeURIComponent(t)}`).join("&");
    const url = `http://localhost:8000/migrate-tables/${sourceDbType}/${selectedSchema}/stream?${queryParams}&include_empty=true&transaction_id=${transactionId}`;

    const eventSource = new EventSource(url);
    eventSourceRef.current = eventSource;

    let completedCount = 0;

    eventSource.onmessage = (event) => {
      if (event.data) {
        const log = parseLogMessage(event.data);
        setMigrationLogs((prev) => [...prev, log]);

        if (event.data.includes("✅") || event.data.includes("❌")) {
          completedCount++;
          setMigrationProgress({ completed: completedCount, total: selectedTables.size });
        }

        if (event.data.includes("Conversion completed") || event.data.includes("Conversion done")) {
          setMigrating(false);
          eventSource.close();
          eventSourceRef.current = null;
        }
      }
    };

    eventSource.onerror = () => {
      setMigrating(false);
      setMigrationLogs((prev) => [...prev, { type: "error", message: "❌ Conversion stream disconnected." }]);
      eventSource.close();
      eventSourceRef.current = null;
    };
  };

  // Poll backend for per-object progress while modal is open
  useEffect(() => {
    let intervalId;
    if (showStatusModal && transactionId) {
      intervalId = setInterval(async () => {
        try {
          const res = await fetch(`http://localhost:8000/migration-status/${transactionId}`);
          if (res.ok) {
            const data = await res.json();
            setObjectsProgress(data.objects || []);
          }
        } catch (err) {
          console.error("Error fetching conversion progress:", err);
        }
      }, 3000);
    }
    return () => {
      if (intervalId) clearInterval(intervalId);
    };
  }, [showStatusModal, transactionId]);

  const handleCloseStatusModal = () => {
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
    setShowStatusModal(false);
    setMigrating(false);
    setSelectedTables(new Set());
    setMigrationProgress({ completed: 0, total: 0 });
    setObjectsProgress([]);
  };

  const filteredTables = schemaObjects.filter((obj) =>
    obj.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="table-migration">
      <Header />
      <NavTabs state={state} />

      <main className={`main-content ${selectedSchema ? "split-view" : ""}`}>
        <div className="content-wrapper">
          <div className="schema-panel">
            <SchemaSelector
              sourceDbType={sourceDbType}
              onSchemaSelect={handleSchemaSelected}
              selectedSchema={selectedSchema}
            />
          </div>

          {selectedSchema && (
            <div className="table-panel">
              <div className="table-header">
                <h4>Tables in {selectedSchema}</h4>
                <div className="top-buttons">
                  <Button
                    size="sm"
                    kind="secondary"
                    disabled={!selectedSchema || txLoading}
                    onClick={() => setShowSchemaModal(true)}
                  >
                    Convert Entire Tables
                  </Button>
                  <Button
                    size="sm"
                    kind="primary"
                    disabled={selectedTables.size === 0 || txLoading}
                    onClick={() => setShowConfirmModal(true)}
                  >
                    Convert Selected Tables
                  </Button>
                </div>
              </div>

              <div className="buttons-container" style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "1rem" }}>
                <div style={{ display: "flex", alignItems: "center" }}>
                  <Tag type="gray" size="sm">{filteredTables.length} tables found</Tag>
                  <label style={{ marginLeft: "1rem" }}>
                    <input
                      type="checkbox"
                      onChange={handleSelectAll}
                      checked={selectedTables.size === filteredTables.length && filteredTables.length > 0}
                      disabled={txLoading}
                    /> Select All
                  </label>
                </div>
                <TextInput
                  size="sm"
                  id="search-table"
                  placeholder="Search tables..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  labelText=""
                  style={{ width: "400px" }}
                  disabled={txLoading}
                />
              </div>

              {loadingObjects ? (
                <InlineLoading description="Loading tables..." />
              ) : filteredTables.length > 0 ? (
                <div className="table-scroll">
                  <table className="data-table">
                    <thead>
                      <tr><th></th><th>#</th><th>Name</th></tr>
                    </thead>
                    <tbody>
                      {filteredTables.map((obj, idx) => (
                        <tr key={obj}>
                          <td>
                            <input
                              type="checkbox"
                              checked={selectedTables.has(obj)}
                              onChange={() => toggleTableSelection(obj)}
                              disabled={txLoading}
                            />
                          </td>
                          <td>{idx + 1}</td>
                          <td>{obj}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p>No tables found.</p>
              )}
            </div>
          )}
        </div>
      </main>

      {/* Confirm Selected Tables Modal */}
      <ComposedModal open={showConfirmModal} onClose={() => setShowConfirmModal(false)} size="sm">
        <ModalHeader title="Confirm Table Conversion" />
        <ModalBody>
          <p>Are you sure you want to convert {selectedTables.size} selected tables?</p>
        </ModalBody>
        <ModalFooter>
          <Button kind="secondary" onClick={() => setShowConfirmModal(false)}>Cancel</Button>
          <Button kind="primary" onClick={handleStartMigration}>Yes, Convert</Button>
        </ModalFooter>
      </ComposedModal>

      {/* Confirm Full Schema Modal */}
      <ComposedModal open={showSchemaModal} onClose={() => setShowSchemaModal(false)} size="sm">
        <ModalHeader title="Confirm Full Schema Conversion" />
        <ModalBody>
          <p>Are you sure you want to convert the entire schema <strong>{selectedSchema}</strong>?</p>
        </ModalBody>
        <ModalFooter>
          <Button kind="secondary" onClick={() => setShowSchemaModal(false)}>Cancel</Button>
          <Button kind="primary" onClick={() => window.location.href = `/full-schema-migration?schema=${selectedSchema}&sourceDbType=${sourceDbType}`}>Go to Full Schema Conversion</Button>
        </ModalFooter>
      </ComposedModal>

      <MigrationStatusModal
        open={showStatusModal}
        onClose={handleCloseStatusModal}
        migrating={migrating}
        title="Conversion Progress"
        progressValue={migrationProgress.completed}
        progressMax={migrationProgress.total}
        progressLabel={`${migrationProgress.completed}/${migrationProgress.total} completed`}
        objectsProgress={objectsProgress} // NEW
        logs={migrationLogs}
        disableClose={true}
      />

      <Footer />
    </div>
  );
};

export default TableMigration;
