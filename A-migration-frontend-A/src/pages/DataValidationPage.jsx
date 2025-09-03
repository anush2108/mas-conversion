import React, { useEffect, useState } from "react";
import {
  Button,
  Column,
  Grid,
  Select,
  SelectItem,
  RadioButtonGroup,
  RadioButton,
  Checkbox,
  ComposedModal,
  ModalHeader,
  ModalBody,
  ModalFooter,
  InlineLoading,
} from "@carbon/react";
import api from "../utils/axiosConfig";
import Header from "../components/Header";
import Footer from "../components/Footer";

const DataValidationPage = () => {
  const [mode, setMode] = useState("schema");
  const [dbType, setDbType] = useState(""); // no default selected

  const [schemas, setSchemas] = useState([]);
  const [selectedSchema, setSelectedSchema] = useState("");

  const [tableList, setTableList] = useState([]);
  const [selectedTables, setSelectedTables] = useState([]);

  const [validationResults, setValidationResults] = useState([]);
  const [validationLogs, setValidationLogs] = useState([]);
  const [isValidating, setIsValidating] = useState(false);
  const [showModal, setShowModal] = useState(false);

  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables, setLoadingTables] = useState(false);

  const [searchTerm, setSearchTerm] = useState("");
  const filteredTables = tableList.filter((table) =>
  table.toLowerCase().includes(searchTerm.toLowerCase())
);

  const logWithTime = (msg) => {
    const timestamp = `[${new Date().toLocaleTimeString()}]`;
    setValidationLogs((prev) => [...prev, `${timestamp} ${msg}`]);
  };

useEffect(() => {
  if (dbType) loadSchemas();
}, [dbType]);


  useEffect(() => {
    if ((mode === "tables" || mode === "table") && selectedSchema) {
      loadTables();
    }
  }, [mode, selectedSchema]);

  const loadSchemas = async () => {
    setLoadingSchemas(true);
    setSchemas([]);
    setSelectedSchema("");
    try {
      const res =
        dbType === "oracle"
          ? await api.get("/o_schemas")
          : await api.get("/s_schemas");

      setSchemas(res.data);
      setSelectedSchema(res.data[0] || "");
    } catch (err) {
      console.error("Failed to load schemas", err);
    } finally {
      setLoadingSchemas(false);
    }
  };

  const loadTables = async () => {
    setLoadingTables(true);
    setTableList([]);
    setSelectedTables([]);
    try {
      const res =
        dbType === "oracle"
          ? await api.get(`/o_schemas/${selectedSchema}`)
          : await api.get(`/s_schemas/${selectedSchema}`);

      setTableList(res.data);
    } catch (err) {
      console.error("Failed to load tables", err);
    } finally {
      setLoadingTables(false);
    }
  };

  const handleValidate = async () => {
    setValidationResults([]); // ✅ Clear old results
    setValidationLogs([]);
    setShowModal(true);
    setIsValidating(true);

    try {
      logWithTime("Starting validation...");
      logWithTime(`Selected DB: ${dbType}`);
      logWithTime(`Selected Schema: ${selectedSchema}`);

      let response;

      if (mode === "schema") {
        logWithTime("Mode: Validating entire schema...");
        logWithTime("⏳ Sending request ...");
        response = await api.post("/validate/schema", {
          source_type: dbType,
          schema: selectedSchema,
        });
      } else if (mode === "tables") {
        logWithTime(`Mode: Validating selected tables: ${selectedTables.join(", ")}`);
        logWithTime("⏳ Sending request ...");
        response = await api.post("/validate/tables", {
          source_type: dbType,
          tables: selectedTables.map((t) => `${selectedSchema}.${t}`),
        });
      } else {
        logWithTime(`Mode: Validating single table: ${selectedTables[0]}`);
        logWithTime("⏳ Sending request ...");
        response = await api.post("/validate/table", {
          source_type: dbType,
          tables: [`${selectedSchema}.${selectedTables[0]}`],
        });
      }

      logWithTime("✔️ Validation complete.");
      setValidationResults(Array.isArray(response.data) ? response.data : [response.data]);
    } catch (err) {
      const rawError = err?.response?.data;
      const errorMsg =
        typeof rawError === "string"
          ? rawError
          : rawError?.detail || err.message || "Unknown error";

      if (errorMsg.includes("SQL0204N")) {
        const match = errorMsg.match(/DB2 table '(.*?)'/i);
        const missingTable = match?.[1] || "unknown";
        logWithTime(`❌ Table "${missingTable}" not found in Db2.`);
        logWithTime("👉 Please convert this table first, then re-run validation.");
      } else {
        logWithTime(`❌ Validation error: ${errorMsg}`);
      }
      console.error("Validation error", err);
    } finally {
      setIsValidating(false);
    }
  };

const handleCheckboxChange = (table, checked) => {
  if (checked) {
    setSelectedTables((prev) =>
      mode === "table" ? [table] : [...prev, table]
    );
  } else {
    setSelectedTables((prev) =>
      mode === "table" ? [] : prev.filter((t) => t !== table)
    );
  }
};



  return (
    <div className="landing-page">
      <Header />

      <section className="landing-options">
        <div className="container">
          <Grid>
            <Column lg={16} md={8} sm={4}>
              <h2 className="section-heading">Data <span className="highlight-blue">Validation</span></h2>
              <p>
                Select source DB type, choose validation scope, and verify
                integrity after Conversion.
              </p>

            <Select
              id="db-select"
              labelText="Select Source Database"
              value={dbType}
              onChange={(e) => setDbType(e.target.value)}
            >
              <SelectItem disabled hidden value="" text="-- Choose Database --" />
              <SelectItem value="oracle" text="Oracle" />
              <SelectItem value="sql" text="SQL Server" />
            </Select>


              <RadioButtonGroup
                legendText="Choose Validation Scope"
                name="validation-mode"
                valueSelected={mode}
                onChange={setMode}
              >
                <RadioButton labelText="Entire Schema" value="schema" id="schema" />
                <RadioButton labelText="Selected Tables" value="tables" id="tables" />
                <RadioButton labelText="Single Table" value="table" id="table" />
              </RadioButtonGroup>

              {/* Schema Selector */}
              <div style={{ marginTop: "1rem" }}>
                {loadingSchemas ? (
                  <InlineLoading description="Loading schemas..." />
                ) : (
                  <Select
                    id="schema-select"
                    labelText="Select Schema"
                    value={selectedSchema}
                    onChange={(e) => setSelectedSchema(e.target.value)}
                  >
                    {schemas.map((schema) => (
                      <SelectItem key={schema} value={schema} text={schema} />
                    ))}
                  </Select>
                )}
              </div>

              {/* Table List */}
{/* Table List with Search and Scroll */}
{(mode === "tables" || mode === "table") && (
  <div style={{ marginTop: "1rem", maxHeight: "200px", overflowY: "auto", border: "1px solid #e0e0e0", borderRadius: "6px", padding: "0.75rem" }}>
    <input
      type="text"
      placeholder="Search tables..."
      value={searchTerm}
      onChange={(e) => setSearchTerm(e.target.value)}
      style={{ width: "100%", padding: "0.5rem", marginBottom: "0.75rem", borderRadius: "4px", border: "1px solid #ccc" }}
    />
    {loadingTables ? (
      <InlineLoading description="Loading tables..." />
    ) : (
      filteredTables.map((table) => (
        <Checkbox
          key={table}
          labelText={table}
          id={table}
          checked={selectedTables.includes(table)}
          onChange={(e, { checked }) => handleCheckboxChange(table, checked)}
          disabled={mode === "table" && selectedTables.length === 1 && !selectedTables.includes(table)}
        />
      ))
    )}
  </div>
)}


<Button
  kind="primary"
  onClick={handleValidate}
  style={{ marginTop: "1.5rem" }}
  disabled={
    !dbType ||
    !selectedSchema ||
    (mode === "tables" && selectedTables.length === 0) ||
    (mode === "table" && selectedTables.length !== 1)
  }
>
  Run Validation
</Button>

            </Column>
          </Grid>
        </div>
      </section>

      <Footer />

      {/* Modal for Logs and Results */}
      <ComposedModal
        open={showModal}
        onClose={() => setShowModal(false)}
        size="lg"
      >
        <ModalHeader title="Validation Progress" />
        <ModalBody>
          {isValidating ? (
            <InlineLoading description="Validating data..." />
          ) : (
            <p>Validation complete.</p>
          )}

          <div
            style={{
              maxHeight: "250px",
              overflowY: "auto",
              background: "#f4f4f4",
              padding: "1rem",
              marginTop: "1rem",
              borderRadius: "4px",
              fontFamily: "monospace",
              fontSize: "0.875rem",
            }}
          >
            {validationLogs.map((log, idx) => (
              <div key={idx}>{log}</div>
            ))}
          </div>

          {!isValidating && validationResults.length > 0 && (
            <div
              style={{
                marginTop: "1.5rem",
                background: "#ffffff",
                border: "1px solid #e0e0e0",
                borderRadius: "6px",
                padding: "1rem",
              }}
            >
              <h5 style={{ marginBottom: "0.5rem" }}>Validation Results</h5>
              {validationResults.map((res, idx) => (
                <div
                  key={idx}
                  style={{
                    marginBottom: "1rem",
                    padding: "0.5rem",
                    background: res.match ? "#e5f6e5" : "#fff1f1",
                    borderLeft: `4px solid ${res.match ? "#24a148" : "#da1e28"}`,
                  }}
                >
                  <strong>{res.table}</strong>:{" "}
                  <span style={{ color: res.match ? "#24a148" : "#da1e28" }}>
                    {res.match ? "✅ Match" : "❌ Mismatch"}
                  </span>
                  <br />
                  Rows: {res.row_count_source} vs {res.row_count_target}
                  <br />
                  Hashes:{" "}
                  <code>{res.source_hash?.slice(0, 8) || "-"}</code> vs{" "}
                  <code>{res.target_hash?.slice(0, 8) || "-"}</code>
                </div>
              ))}
            </div>
          )}
        </ModalBody>
        <ModalFooter>
          <Button kind="secondary" onClick={() => setShowModal(false)}>
            Close
          </Button>
        </ModalFooter>
      </ComposedModal>
    </div>
  );
};

export default DataValidationPage;
