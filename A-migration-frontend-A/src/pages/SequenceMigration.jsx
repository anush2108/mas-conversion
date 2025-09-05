import React, { useState, useRef } from "react";
import { useLocation } from "react-router-dom";
import {
  Button,
  InlineLoading,
  TextInput,
} from "@carbon/react";
import Header from "../components/Header";
import Footer from "../components/Footer";
import NavTabs from "../components/NavTabs";
import SchemaSelector from "../components/SchemaSelector";
import MigrationStatusModal from "../components/MigrationStatusModal";
import { useTransactionIdForSchema } from "../hooks/useTransactionIdForSchema";
import "../styles/SequenceMigration.css";

const SequenceMigration = () => {
  const { state } = useLocation();
  const sourceDbType = state?.sourceDbType?.toLowerCase();

  const [selectedSchema, setSelectedSchema] = useState("");
  const { transactionId, loading: txLoading } = useTransactionIdForSchema(selectedSchema);

  const [sequences, setSequences] = useState([]);
  const [loadingSequences, setLoadingSequences] = useState(false);

  const [migrationLogs, setMigrationLogs] = useState([]);
  const [migrationProgress, setMigrationProgress] = useState({ completed: 0, total: 0 });
  const [migrating, setMigrating] = useState(false);
  const [showModal, setShowModal] = useState(false);

  const [searchTerm, setSearchTerm] = useState("");

  const eventSourceRef = useRef(null);

  const apiPrefix = sourceDbType === "oracle" ? "/oracle" : "/sql";

  // Fetch sequences for the selected schema
  const fetchSequences = async (schema) => {
    setLoadingSequences(true);
    try {
      // Fixed to match backend
      const res = await fetch(`https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/sequences${apiPrefix}/${schema}/list`);
      const data = await res.json();
      setSequences(Array.isArray(data.sequences) ? data.sequences : []);
    } catch (err) {
      console.error("Failed to load sequences", err);
      setSequences([]);
    } finally {
      setLoadingSequences(false);
    }
  };

  // Handle schema selection
  const handleSchemaSelected = (schema) => {
    setSelectedSchema(schema);
    setSequences([]);
  };

  // Fetch sequences upon schema change
  React.useEffect(() => {
    if (selectedSchema) {
      fetchSequences(selectedSchema);
    } else {
      setSequences([]);
    }
  }, [selectedSchema]);

  // Start migration with SSE streaming
  const startMigration = () => {
    if (!selectedSchema || !sourceDbType || !transactionId) return;
    if (migrating) return;

    setMigrationLogs([]);
    setMigrationProgress({ completed: 0, total: 0 });
    setShowModal(true);
    setMigrating(true);

    // Clean up any existing SSE connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    const url = `https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/sequences/migrate/stream?source_type=${sourceDbType}&schema=${selectedSchema}&transaction_id=${transactionId}`;

    const es = new EventSource(url);
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.progress) {
          setMigrationProgress(data.progress);
        }
        if (data.logs && data.logs.length > 0) {
          setMigrationLogs((prev) => [...prev, ...data.logs]);
        }
        // When complete, stop migration
        if (data.progress?.completed === data.progress?.total) {
          setMigrating(false);
          if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
          }
        }
      } catch (error) {
        console.error("Failed to parse SSE message:", error);
      }
    };

    es.onerror = () => {
      console.error("SSE error");
      setMigrating(false);
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  };

  // Close modal & cleanup
  const handleClose = () => {
    setShowModal(false);
    setMigrating(false);
    setMigrationLogs([]);
    setMigrationProgress({ completed: 0, total: 0 });
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
  };

  // Clear logs handler
  const clearLogs = () => setMigrationLogs([]);

  // Filter sequences by search term (case insensitive)
  const filteredSequences = sequences.filter((seq) =>
    seq.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="sequence-migration page-wrapper">
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
            <div className="sequence-panel">
              <div className="sequence-header">
                <h4>Sequences in {selectedSchema}</h4>
                <Button
                  kind="primary"
                  size="sm"
                  disabled={migrating || loadingSequences || txLoading || !transactionId}
                  onClick={startMigration}
                >
                  {migrating ? "Converting..." : "Convert All"}
                </Button>
              </div>

              <div style={{ marginBottom: "1rem" }}>
                <TextInput
                  id="search-sequences"
                  labelText="Search Sequences"
                  placeholder="Type sequence name..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  disabled={migrating}
                />
              </div>

              {/* Only change: make scroll work by matching class to CSS */}
              <div className="sequence-scroll">
                {loadingSequences ? (
                  <div style={{ marginTop: 20 }}>
                    <InlineLoading description="Loading sequences..." />
                  </div>
                ) : filteredSequences.length === 0 ? (
                  <p>No sequences found.</p>
                ) : (
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th className="index-col">#</th>
                        <th className="name-col">Sequence Name</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredSequences.map((seq, idx) => (
                        <tr key={seq}>
                          <td className="index-col">{idx + 1}</td>
                          <td className="name-col">{seq}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          )}
        </div>
      </main>

      <MigrationStatusModal
        open={showModal}
        onClose={handleClose}
        migrating={migrating}
        title="Sequence Conversion Status"
        progressValue={migrationProgress.completed}
        progressMax={migrationProgress.total}
        progressLabel={`${migrationProgress.completed} / ${migrationProgress.total} completed`}
        logs={migrationLogs}
        disableClose={true}
        extraHeaderContent={
          <Button
            kind="tertiary"
            size="sm"
            disabled={migrating || migrationLogs.length === 0}
            onClick={clearLogs}
            style={{ marginLeft: "auto" }}
          >
            Clear Logs
          </Button>
        }
      />

      <Footer />
    </div>
  );
};

export default SequenceMigration;
