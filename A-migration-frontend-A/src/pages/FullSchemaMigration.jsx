import React, { useState, useRef, useCallback } from "react";
import { useLocation } from "react-router-dom";
import {
  Button,
  ComposedModal,
  ModalHeader,
  ModalBody,
  ModalFooter,
  Tag,
} from "@carbon/react";

import Header from "../components/Header";
import Footer from "../components/Footer";
import SchemaSelector from "../components/SchemaSelector";
import "../styles/FullSchemaMigration.css";
import NavTabs from "../components/NavTabs";
import MigrationStatusModal from "../components/MigrationStatusModal";

import { useTransactionIdForSchema } from "../hooks/useTransactionIdForSchema";
import { useMigrationStatus } from "../hooks/useMigrationStatus"; // polling hook

const FullSchemaMigration = () => {
  const { state } = useLocation();
  const sourceDbType = state?.sourceDbType?.toLowerCase();

  const [selectedSchema, setSelectedSchema] = useState("");
  const { transactionId, loading: txLoading } = useTransactionIdForSchema(selectedSchema);

  const [showSchemaModal, setShowSchemaModal] = useState(false);
  const [showStatusModal, setShowStatusModal] = useState(false);
  const [migrating, setMigrating] = useState(false);
  const [migrationLogs, setMigrationLogs] = useState([]);
  const [migrationProgress, setMigrationProgress] = useState(0);
  const [currentPhase, setCurrentPhase] = useState("Initializing");
  const [currentTable, setCurrentTable] = useState("");
  const [migrationStats, setMigrationStats] = useState({
    tables: { completed: 0, total: 0 },
    rows: 0,
    phase: "Initializing",
  });

  const eventSourceRef = useRef(null);
  const lastActivityRef = useRef(Date.now());

  // Polling migration status API when modal is open
  const { status: migrationStatus, refresh: refreshStatus } =
    useMigrationStatus(showStatusModal ? transactionId : null, sourceDbType, selectedSchema);

  const handleSchemaSelected = useCallback((schema) => {
    setSelectedSchema(schema);
  }, []);

  const parseLogMessage = useCallback(
    (message) => {
      const updatedStats = { ...migrationStats };

      if (message.includes("Migrating") && message.includes("attempt")) {
        const match = message.match(/Migrating\s+([^\s]+)/i);
        if (match) setCurrentTable(match[1]);
      }

      if (message.includes("📦 Starting table") || message.includes("📦 Converting")) {
        setCurrentPhase("Converting Tables");
        updatedStats.phase = "Tables";
      } else if (message.includes("🪟 Converting views")) {
        setCurrentPhase("Converting Views");
        updatedStats.phase = "Views";
      } else if (message.includes("🔢 Converting sequences")) {
        setCurrentPhase("Converting Sequences");
        updatedStats.phase = "Sequences";
      } else if (message.includes("🎯 Converting triggers")) {
        setCurrentPhase("Converting Triggers");
        updatedStats.phase = "Triggers";
      } else if (message.includes("📐 Converting indexes")) {
        setCurrentPhase("Converting Indexes");
        updatedStats.phase = "Indexes";
      }

      if (message.match(/\d+\s+tables/)) {
        const match = message.match(/(\d+)\s+tables/i);
        if (match) updatedStats.tables.total = parseInt(match[1], 10);
      }

      if (/rows in\b/.test(message)) {
        const rowMatch = message.match(/^✅ .*?:\s*([\d,]+) rows/i);
        if (rowMatch) {
          const rows = parseInt(rowMatch[1].replace(/,/g, ""), 10);
          updatedStats.rows += rows;
        }
      }

      if (message.includes("Conversion completed") && message.includes("rows")) {
        const match = message.match(/(\d[\d,]*)\s+rows/i);
        if (match) updatedStats.rows = parseInt(match[1].replace(/,/g, ""), 10);
      }

      if (message.includes("Progress:") && message.includes("%")) {
        const progressMatch = message.match(/(\d+(?:\.\d+)?)%.*\((\d+)\/(\d+)\)/);
        if (progressMatch) {
          const [, percent, completed, total] = progressMatch;
          setMigrationProgress(parseFloat(percent));
          updatedStats.tables.completed = parseInt(completed || 0, 10);
          updatedStats.tables.total = parseInt(total || updatedStats.tables.total, 10);
        }
      }

      setMigrationStats(updatedStats);

      if (/✅/.test(message)) return { type: "success", message };
      if (/❌/.test(message)) return { type: "error", message };
      if (/⚠️/.test(message)) return { type: "warning", message };
      if (/⏰ TIMEOUT/i.test(message) || /⚠️ STALL DETECTED/i.test(message)) return { type: "error", message };
      if (/Migration stalled.*rows/i.test(message)) return { type: "warning", message };
      if (/📦|🪟|🔢|🎯|📐|🔄|🚀|📊/.test(message)) return { type: "info", message };

      return { type: "default", message };
    },
    [migrationStats]
  );

  const handleMigrateSchema = () => {
    if (!selectedSchema || !sourceDbType || !transactionId) return;

    setShowSchemaModal(false);
    setMigrating(true);
    setMigrationLogs([]);
    setMigrationProgress(0);

    setCurrentPhase("Initializing");
    setCurrentTable("");
    setMigrationStats({ tables: { completed: 0, total: 0 }, rows: 0, phase: "Initializing" });
    setShowStatusModal(true);
    lastActivityRef.current = Date.now();

    const eventSourceUrl = `https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/full-migration/all/stream?source_type=${sourceDbType}&schema=${selectedSchema}&transaction_id=${transactionId}`;

    if (eventSourceRef.current) eventSourceRef.current.close();

    const eventSource = new EventSource(eventSourceUrl);
    eventSourceRef.current = eventSource;

    eventSource.onmessage = (event) => {
      lastActivityRef.current = Date.now();

      if (event.data.includes("✅ Full schema conversion completed")) {
        setCurrentPhase("Completed");
        setMigrationProgress(100);
        setMigrationStats((prev) => ({ ...prev, phase: "Completed" }));

        setTimeout(() => {
          setMigrating(false);
          eventSourceRef.current?.close();
          eventSourceRef.current = null;
        }, 500);
      }

      const log = parseLogMessage(event.data);
      if (log) setMigrationLogs((prev) => [...prev, log]);

      refreshStatus();
    };

    eventSource.onerror = () => {
      setMigrating(false);
      setCurrentPhase("Disconnected");
      setMigrationLogs((prev) => [
        ...prev,
        { type: "error", message: "❌ Conversion stream disconnected unexpectedly." }
      ]);
      eventSourceRef.current?.close();
      eventSourceRef.current = null;
      refreshStatus();
    };
  };

  const handleCloseStatusModal = () => {
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
    setShowStatusModal(false);
    setMigrating(false);
    setCurrentPhase("");
    setCurrentTable("");
  };

  const getLogMessageStyle = (type) => {
    switch (type) {
      case "success": return { color: "#24a148", fontWeight: "500" };
      case "error": return { color: "#da1e28", fontWeight: "500" };
      case "warning": return { color: "#f1c21b", fontWeight: "500" };
      case "info": return { color: "#0f62fe", fontWeight: "500" };
      default: return { color: "#525252" };
    }
  };

  const objectsProgress = migrationStatus
    ? Object.entries(migrationStatus.done_counts).map(([type, data]) => ({
        type,
        success: data.success_count,
        total: data.total,
        errors: data.errors
      }))
    : [];

  return (
    <div className="full-schema-migration page-container">
      <Header />
      <NavTabs state={state} />

      <main className="main-content">
        <SchemaSelector sourceDbType={sourceDbType} onSchemaSelect={handleSchemaSelected} selectedSchema={selectedSchema} />

        <div style={{ marginTop: "2rem", marginLeft: "3rem" }}>
          <h4>Full Schema Conversion</h4>
          <div style={{ marginBottom: "1rem", display: "flex", justifyContent: "space-between" }}>
            <div>
              {selectedSchema && <Tag type="gray" size="sm">Selected Schema: {selectedSchema}</Tag>}
              {currentTable && <Tag type="cyan" size="sm">Current Table: {currentTable}</Tag>}
              {transactionId && <Tag type="magenta" size="sm" style={{ marginLeft: "0.5rem" }}>Transaction ID: {transactionId}</Tag>}
            </div>
            <div>
              <Button kind="primary" size="sm" disabled={!selectedSchema || txLoading} onClick={() => setShowSchemaModal(true)}>
                Convert Entire Schema
              </Button>
            </div>
          </div>
        </div>
      </main>

      {/* Confirm Modal */}
      <ComposedModal open={showSchemaModal} onClose={() => setShowSchemaModal(false)} size="sm">
        <ModalHeader title="Confirm Full Schema Conversion" />
        <ModalBody>
          <p>Are you sure you want to convert the entire schema <strong>{selectedSchema}</strong>?</p>
        </ModalBody>
        <ModalFooter>
          <Button kind="secondary" onClick={() => setShowSchemaModal(false)}>Cancel</Button>
          <Button kind="primary" onClick={handleMigrateSchema}>Yes, Convert</Button>
        </ModalFooter>
      </ComposedModal>

      {/* Status Modal */}
      <MigrationStatusModal
        open={showStatusModal}
        onClose={handleCloseStatusModal}
        migrating={migrating}
        title="Conversion Progress"
        progressValue={migrationStatus?.overall?.percentage || migrationProgress}
        progressMax={100}
        progressLabel={
          migrationStatus
            ? `${migrationStatus.overall.percentage}% Completed`
            : `${migrationProgress.toFixed(1)}% Completed`
        }
        objectsProgress={objectsProgress}
        logs={migrationLogs}
        getLogStyle={getLogMessageStyle}
        disableClose={true}
        extraHeaderContent={
          <div style={{ display: "flex", gap: "1rem", marginTop: "0.5rem" }}>
            <span>Phase: {currentPhase}</span>
            {currentTable && <span>Current Table: {currentTable}</span>}
            {migrationStatus && (
              <span>Overall: {migrationStatus.overall.done}/{migrationStatus.overall.total}</span>
            )}
          </div>
        }
        inlineNotification={{
          kind: "info",
          title: "Conversion Status",
          subtitle: `Last activity: ${new Date(lastActivityRef.current).toLocaleTimeString()}`,
          hideCloseButton: true,
        }}
      />

      <Footer />
    </div>
  );
};

export default FullSchemaMigration;
