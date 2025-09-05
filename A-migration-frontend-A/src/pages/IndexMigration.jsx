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
import "../styles/IndexMigration.css";

const IndexMigration = () => {
  const { state } = useLocation();
  const sourceDbType = state?.sourceDbType?.toLowerCase();
  const [selectedSchema, setSelectedSchema] = useState("");
  const { transactionId, loading: txLoading } = useTransactionIdForSchema(selectedSchema);

  const [indexes, setIndexes] = useState([]);
  const [filteredIndexes, setFilteredIndexes] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedIndexes, setSelectedIndexes] = useState(new Set());
  const [loadingIndexes, setLoadingIndexes] = useState(false);
  const [showModal, setShowModal] = useState(false);
  const [migrationLogs, setMigrationLogs] = useState([]);
  const [migrating, setMigrating] = useState(false);

  const indexApiPrefix = "https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/indexes";

  const fetchIndexes = async (schema) => {
    setSelectedSchema(schema);
    setLoadingIndexes(true);
    try {
      const res = await fetch(`${indexApiPrefix}/list?source_type=${sourceDbType}&schema=${schema}`);
      const data = await res.json();
      const indexList = Array.isArray(data)
        ? data.map((i) => i.index || i.name || i)
        : [];
      setIndexes(indexList);
      setFilteredIndexes(indexList);
      setSelectedIndexes(new Set());
    } catch (err) {
      console.error(err);
      setIndexes([]);
      setFilteredIndexes([]);
    } finally {
      setLoadingIndexes(false);
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
        ? `&${Array.from(selectedIndexes)
            .map((i) => `index_names=${encodeURIComponent(i)}`)
            .join("&")}`
        : "";
      const url = `${indexApiPrefix}/migrate?source_type=${sourceDbType}&target=db2&schema=${selectedSchema}&transaction_id=${transactionId}${names}`;
      const response = await fetch(url, { method: "POST" });
      const result = await response.json();

      const logs = [
        `✅ Converted: ${result.total_migrated} / ${result.total_requested}`,
        ...(result.migrated || []).map((i, idx) => `✅ ${idx + 1}. ${i}`),
        ...(result.skipped || []).map(
          (s, idx) =>
            `❌ ${result.migrated.length + idx + 1}. ${s.index} - ${s.reason || "Unknown error"}`
        ),
      ];
      setMigrationLogs(logs);
      setSelectedIndexes(new Set());
    } catch (err) {
      setMigrationLogs([`❌ Error: ${err.message || err}`]);
    } finally {
      setMigrating(false);
    }
  };

  const toggleIndexSelection = (index) => {
    setSelectedIndexes((prev) => {
      const updated = new Set(prev);
      updated.has(index) ? updated.delete(index) : updated.add(index);
      return updated;
    });
  };

  const handleSearch = (value) => {
    setSearchTerm(value);
    const lowerValue = value.toLowerCase();
    setFilteredIndexes(indexes.filter((i) => i.toLowerCase().includes(lowerValue)));
  };

  const handleCloseModal = () => {
    setShowModal(false);
    setMigrationLogs([]);
    setMigrating(false);
  };

  return (
    <div className="index-migration">
      <Header />
      <NavTabs state={state} />

      <main className={`main-content ${selectedSchema ? "split-view" : ""}`}>
        <div className="content-wrapper">
          <div className="schema-panel">
            <SchemaSelector
              sourceDbType={sourceDbType}
              onSchemaSelect={fetchIndexes}
              selectedSchema={selectedSchema}
            />
          </div>

          {selectedSchema && (
            <div className="index-panel">
              <div className="index-header">
                <h4>Indexes in {selectedSchema}</h4>
                <div className="button-actions">
                  <Button kind="secondary" size="sm" onClick={() => handleMigration(false)} disabled={txLoading}>
                    Convert Entire Indexes
                  </Button>
                  <Button
                    kind="primary"
                    size="sm"
                    onClick={() => handleMigration(true)}
                    disabled={selectedIndexes.size === 0 || txLoading}
                  >
                    Convert Selected Indexes
                  </Button>
                </div>
              </div>

              <div className="count-select">
                <span>{filteredIndexes.length} indexes found</span>
                <label>
                  <input
                    type="checkbox"
                    onChange={(e) =>
                      setSelectedIndexes(
                        e.target.checked ? new Set(filteredIndexes) : new Set()
                      )
                    }
                    checked={
                      selectedIndexes.size === filteredIndexes.length &&
                      filteredIndexes.length > 0
                    }
                    disabled={txLoading}
                  />
                  {" "}
                  Select All
                </label>
              </div>

              <div className="search-bar-container">
                <TextInput
                  id="search-indexes"
                  labelText=""
                  placeholder="Search indexes..."
                  value={searchTerm}
                  onChange={(e) => handleSearch(e.target.value)}
                  disabled={txLoading}
                />
              </div>

              {loadingIndexes ? (
                <InlineLoading description="Loading indexes..." />
              ) : filteredIndexes.length > 0 ? (
                <div className="index-scroll">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th></th>
                        <th>#</th>
                        <th>Index Name</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredIndexes.map((index, idx) => (
                        <tr key={index}>
                          <td>
                            <Checkbox
                              id={index}
                              labelText=""
                              checked={selectedIndexes.has(index)}
                              onChange={() => toggleIndexSelection(index)}
                              disabled={txLoading}
                            />
                          </td>
                          <td>{idx + 1}</td>
                          <td>{index}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p>No indexes found.</p>
              )}
            </div>
          )}
        </div>
      </main>

      <ComposedModal open={showModal} onClose={handleCloseModal} size="lg">
        <ModalHeader title="Index Conversion Status" closeModal={handleCloseModal} />
        <ModalBody>
          {migrationLogs.length === 0 ? (
            <InlineLoading description="Starting Conversion..." />
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

export default IndexMigration;
