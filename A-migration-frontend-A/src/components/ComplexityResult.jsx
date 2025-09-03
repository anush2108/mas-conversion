import React, { useEffect, useState } from "react";
import { Button, Loading, Select, SelectItem } from "@carbon/react";
import {
  Package,
  FolderOpen,
  Tools,
  DocumentTasks,
  DataStructured,
} from "@carbon/icons-react";
import "../styles/ComplexityResult.css";

const API_BASE = process.env.REACT_APP_API_BASE || "https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com";
const COMPLEXITY_MAP = {
  "Low Complexity": "Low",
  "Medium Complexity": "Medium",
  "High Complexity": "High",
  low: "Low",
  medium: "Medium",
  high: "High",
};

const ComplexityResult = ({ dbType, onClose }) => {
  const [loading, setLoading] = useState(false);
  const [loadingMessage, setLoadingMessage] = useState("");
  const [schemas, setSchemas] = useState([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [metrics, setMetrics] = useState(null);
  const [complexity, setComplexity] = useState("");
  const [probability, setProbability] = useState(null);
  const [error, setError] = useState(null);
  const [step, setStep] = useState("select");

  // Fetch schema list
  useEffect(() => {
    const fetchSchemas = async () => {
      setLoading(true);
      setLoadingMessage("Loading Schemas...");
      setError(null);
      try {
        const url =
          dbType === "Oracle"
            ? `${API_BASE}/o_schemas`
            : `${API_BASE}/s_schemas`;
        const res = await fetch(url);
        if (!res.ok) throw new Error(`Failed to load schema list (${res.status})`);
        const data = await res.json();
        if (Array.isArray(data)) {
          setSchemas(data);
        } else {
          throw new Error("Invalid schema list format");
        }
      } catch (err) {
        setError(err.message || "Error fetching schema list");
      } finally {
        setLoading(false);
      }
    };
    if (dbType) fetchSchemas();
  }, [dbType]);

  // Main compute function
  const handleCompute = async (schemaArg) => {
    const schema = schemaArg || selectedSchema;
    if (!schema) return;

    setLoading(true);
    setLoadingMessage("Computing the Complexity...");
    setError(null);
    setComplexity("");
    try {
      // Step 1: Fetch metrics
      const metricsUrl =
        dbType === "Oracle"
          ? `${API_BASE}/migration_complexity/fetch-oracle-db-values/${encodeURIComponent(schema)}`
          : `${API_BASE}/migration_complexity/fetch-mssql-db-values/${encodeURIComponent(schema)}`;
      const metricsRes = await fetch(metricsUrl);
      if (!metricsRes.ok)
        throw new Error(`Metrics fetch failed: ${await metricsRes.text()}`);
      const metricsData = await metricsRes.json();
      setMetrics(metricsData);

      // Step 2: Prediction API
      const predictionUrl =
        dbType === "Oracle"
          ? `${API_BASE}/migration_complexity/predict-oracle-from-db?schema=${encodeURIComponent(schema)}`
          : `${API_BASE}/migration_complexity/predict-mssql-from-db?schema=${encodeURIComponent(schema)}`;
      const predictionRes = await fetch(predictionUrl);
      const predictionData = await predictionRes.json();

      if (!predictionRes.ok) {
        setError(JSON.stringify(predictionData));
        return;
      }

      // Parse prediction
      let label = "";
      let probabilityArr = null;
      if (
        predictionData.prediction ||
        predictionData.probability
      ) {
        label = predictionData.prediction;
        probabilityArr = predictionData.probability;
      } else if (
        predictionData.predictions &&
        predictionData.predictions[0] &&
        Array.isArray(predictionData.predictions[0].values) &&
        predictionData.predictions[0].values.length > 0
      ) {
        const valuesRow = predictionData.predictions[0].values[0];
        label = valuesRow[0]; // prediction string
        probabilityArr = valuesRow[1]; // probability array
      }

      if (label) {
        const cleanLabel = String(label).trim();
        setComplexity(COMPLEXITY_MAP[cleanLabel] || cleanLabel);
      }
      if (Array.isArray(probabilityArr)) setProbability(probabilityArr);

      setStep("result");
    } catch (err) {
      setError(err.message || "Unknown error occurred.");
    } finally {
      setLoading(false);
    }
  };

  const getColorForComplexity = (level) => {
    const strLevel = String(level || "").toLowerCase();
    switch (strLevel) {
      case "low":
        return "#4CAF50";
      case "medium":
        return "#FF9800";
      case "high":
        return "#F44336";
      default:
        return "#777";
    }
  };

  return (
    <div className="complexity-container">
      {loading && (
        <div
          className="loading-overlay"
          style={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "150px",
            textAlign: "center",
          }}
        >
          <Loading withOverlay={false} />
          <p style={{ marginTop: "10px", fontWeight: "500" }}>
            {loadingMessage}
          </p>
        </div>
      )}

      {error && (
        <div className="error-text">
          <strong>Error:</strong> {error}
        </div>
      )}

      {step === "select" && !loading && schemas.length > 0 && (
        <>
          <h3>Select a Schema</h3>
          <Select
            id="schema-select"
            labelText="Schema"
            value={selectedSchema}
            onChange={(e) => {
              setSelectedSchema(e.target.value);
              if (e.target.value) setError(null);
            }}
          >
            <SelectItem value="" text="-- Select Schema --" />
            {schemas.map((sch, idx) => (
              <SelectItem key={idx} value={sch} text={sch} />
            ))}
          </Select>

          <div className="footer-actions">
            <Button kind="secondary" onClick={onClose} className="footer-btn">
              Back
            </Button>
            <Button
              kind="primary"
              onClick={() => handleCompute(selectedSchema)}
              className="footer-btn"
              disabled={!selectedSchema}
            >
              Compute Complexity
            </Button>
          </div>
        </>
      )}

      {step === "result" && !loading && metrics && complexity && (
        <div className="result-wrapper">
          <div className="result-card">
            <h3 style={{ marginTop: 0, marginBottom: "22px" }}>
              Database Metrics
            </h3>
            <div className="metrics-vertical">
              <div className="metric-row">
                <span className="metric-icon"><Package size={24} /></span>
                <span className="metric-label">Data Volume (GB)</span>
                <span className="metric-value">{metrics.data_volume_gb}</span>
              </div>
              <div className="metric-row">
                <span className="metric-icon"><FolderOpen size={24} /></span>
                <span className="metric-label">Tables</span>
                <span className="metric-value">{metrics.num_tables}</span>
              </div>
              <div className="metric-row">
                <span className="metric-icon"><Tools size={24} /></span>
                <span className="metric-label">Indexes</span>
                <span className="metric-value">{metrics.num_indexes}</span>
              </div>
              <div className="metric-row">
                <span className="metric-icon"><DocumentTasks size={24} /></span>
                <span className="metric-label">Work Orders</span>
                <span className="metric-value">{metrics.workorder_records}</span>
              </div>
              <div className="metric-row">
                <span className="metric-icon"><DataStructured size={24} /></span>
                <span className="metric-label">BLOB/CLOB</span>
                <span className="metric-value">{metrics.blobclob_records}</span>
              </div>
            </div>
            <div
              className="complexity-result-box"
              style={{
                backgroundColor: getColorForComplexity(complexity),
              }}
            >
              Conversion Complexity:{" "}
              <span className="complexity-level">{complexity}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ComplexityResult;
