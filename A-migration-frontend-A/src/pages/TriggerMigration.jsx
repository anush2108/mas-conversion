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
import "../styles/TriggerMigration.css";

const TriggerMigration = () => {
  const { state } = useLocation();
  const sourceDbType = state?.sourceDbType?.toLowerCase();

  const [selectedSchema, setSelectedSchema] = useState("");
  const { transactionId, loading: txLoading, error: txError } =
    useTransactionIdForSchema(selectedSchema);

  const [triggers, setTriggers] = useState([]);
  const [selectedTriggers, setSelectedTriggers] = useState(new Set());
  const [loadingTriggers, setLoadingTriggers] = useState(false);
  const [showModal, setShowModal] = useState(false);
  const [migrationLogs, setMigrationLogs] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");

  const triggerApiPrefix = "http://localhost:8000/triggers";

  const fetchTriggers = async (schema) => {
    setSelectedSchema(schema);
    setLoadingTriggers(true);
    try {
      const res = await fetch(
        `${triggerApiPrefix}/list?source_type=${sourceDbType}&schema=${schema}`
      );
      const data = await res.json();
      const triggerList = Array.isArray(data)
        ? data.map((t) => t.trigger || t.trigger_name || t)
        : [];
      setTriggers(triggerList);
      setSelectedTriggers(new Set());
    } catch (err) {
      console.error(err);
      setTriggers([]);
    } finally {
      setLoadingTriggers(false);
    }
  };

  const handleMigration = async (onlySelected = false) => {
    setShowModal(true);
    setMigrationLogs([]);
    if (!transactionId) {
      setMigrationLogs(["❌ No Transaction ID available."]);
      return;
    }
    try {
      const names =
        onlySelected && selectedTriggers.size > 0
          ? `&${Array.from(selectedTriggers).map(t => `trigger_names=${encodeURIComponent(t)}`).join('&')}`
          : "";
      // ALWAYS pass transaction_id
      const url = `${triggerApiPrefix}/migrate?source_type=${sourceDbType}&target=db2&schema=${selectedSchema}&transaction_id=${transactionId}${names}`;
      const response = await fetch(url, { method: "POST" });
      const result = await response.json();

      const logs = [];
      logs.push(`✅ Converted: ${result.total_migrated} / ${result.total_requested}`);
      if (result.migrated) {
        logs.push(...result.migrated.map((t, i) => `✅ ${i + 1}. ${t}`));
      }
      if (result.skipped) {
        logs.push(
          ...result.skipped.map(
            (s, i) =>
              `❌ ${result.migrated.length + i + 1}. ${s.trigger} - ${s.reason || "Unknown error"}`
          )
        );
      }
      setMigrationLogs(logs);
      setSelectedTriggers(new Set());
    } catch (err) {
      setMigrationLogs([`❌ Error: ${err.message || err}`]);
    }
  };

  const handleCloseModal = () => {
    setShowModal(false);
    setMigrationLogs([]);
  };

  const toggleTriggerSelection = (trigger) => {
    setSelectedTriggers((prev) => {
      const updated = new Set(prev);
      updated.has(trigger) ? updated.delete(trigger) : updated.add(trigger);
      return updated;
    });
  };

  const filteredTriggers = triggers.filter((t) =>
    t.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="trigger-migration">
      <Header />
      <NavTabs state={state} />

      <main className={`main-content ${selectedSchema ? "split-view" : ""}`}>
        <div className="content-wrapper">
          <div className="schema-panel">
            <SchemaSelector
              sourceDbType={sourceDbType}
              onSchemaSelect={fetchTriggers}
              selectedSchema={selectedSchema}
            />
          </div>

          {selectedSchema && (
            <div className="trigger-panel">
              <div className="trigger-header">
                <h4>Triggers in {selectedSchema}</h4>
                <div className="button-actions">
                  <Button
                    kind="secondary"
                    size="sm"
                    onClick={() => handleMigration(false)}
                    disabled={txLoading}
                  >
                    Convert Entire Triggers
                  </Button>
                  <Button
                    kind="primary"
                    size="sm"
                    onClick={() => handleMigration(true)}
                    disabled={selectedTriggers.size === 0 || txLoading}
                  >
                    Convert Selected Triggers
                  </Button>
                </div>
              </div>
<div className="count-select">
  <span>{triggers.length} triggers found</span>
  <label className="select-all-inline">
    <input
      type="checkbox"
      onChange={(e) =>
        setSelectedTriggers(
          e.target.checked ? new Set(triggers) : new Set()
        )
      }
      checked={
        selectedTriggers.size === triggers.length &&
        triggers.length > 0
      }
      style={{ marginLeft: "12px", marginRight: "6px" }}
    />
    Select All
  </label>
</div>



              <div style={{ marginBottom: "1rem" }}>
                <TextInput
                  id="search-trigger"
                  labelText="Search Triggers"
                  placeholder="Enter trigger name"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>

              {loadingTriggers ? (
                <InlineLoading description="Loading triggers..." />
              ) : filteredTriggers.length > 0 ? (
                <div className="trigger-scroll">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th></th>
                        <th>#</th>
                        <th>Trigger Name</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTriggers.map((trigger, idx) => (
                        <tr key={trigger}>
                          <td>
                            <Checkbox
                              id={trigger}
                              labelText=""
                              checked={selectedTriggers.has(trigger)}
                              onChange={() => toggleTriggerSelection(trigger)}
                            />
                          </td>
                          <td>{idx + 1}</td>
                          <td>{trigger}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p>No triggers found.</p>
              )}
            </div>
          )}
        </div>
      </main>

      <ComposedModal
        open={showModal}
        onClose={handleCloseModal}
        selectorPrimaryFocus="[data-modal-primary-focus]"
        preventCloseOnClickOutside={false}
        size="lg"
      >
        <ModalHeader title="Trigger Conversion Status" closeModal={handleCloseModal} />
        <ModalBody>
          {migrationLogs.length === 0 ? (
            <InlineLoading description="Converting triggers..." />
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
          <Button
            kind="secondary"
            onClick={handleCloseModal}
            data-modal-primary-focus
          >
            Close
          </Button>
        </ModalFooter>
      </ComposedModal>

      <Footer />
    </div>
  );
};

export default TriggerMigration;
