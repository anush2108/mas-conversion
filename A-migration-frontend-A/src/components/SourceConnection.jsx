import React, { useState, useEffect } from "react";
import {
  Modal,
  Grid,
  Column,
  TextInput,
  Select,
  SelectItem,
  Button,
  InlineNotification,
  RadioButtonGroup,
  RadioButton,
} from "@carbon/react";
import "../styles/SourceConnection.css";
import ComplexityResult from "../components/ComplexityResult";

// Helper: Default connection fields
const DEFAULT_DETAILS = {
  host: "",
  port: "",
  username: "",
  password: "",
  database: "",
  sid: "",
  service_name: "",
  security: "",
};

const SourceConnection = ({
  open,
  onClose,
  onNext,
  sourceDbType,
  disableDbTypeSelect,
  isAIMode = false,
}) => {
  // Always rely on prop for DB type if select is disabled
  const effectiveDbType = disableDbTypeSelect
    ? sourceDbType || "Oracle"
    : undefined;

  const [dbType, setDbType] = useState(sourceDbType || "Oracle");
  const [oracleConnectionType, setOracleConnectionType] = useState("service_name");
  const [sourceDetails, setSourceDetails] = useState({ ...DEFAULT_DETAILS });
  const [error, setError] = useState("");
  const [successMsg, setSuccessMsg] = useState("");
  const [connectionTested, setConnectionTested] = useState(false);
  const [loading, setLoading] = useState(false);
  const [showHelloMessage, setShowHelloMessage] = useState(false);

  // Reset form on open and set DB type
  useEffect(() => {
    if (open) {
      setSourceDetails({ ...DEFAULT_DETAILS });
      setOracleConnectionType("service_name");
      setError("");
      setSuccessMsg("");
      setConnectionTested(false);
      setLoading(false);
      setShowHelloMessage(false);
      // If select is disabled, use external prop; else keep own state
      if (disableDbTypeSelect) {
        setDbType(sourceDbType || "Oracle");
      }
    }
  }, [open, sourceDbType, disableDbTypeSelect]);

  // Ensure correct DB type is used for fields
  const currentDbType = disableDbTypeSelect
    ? sourceDbType || "Oracle"
    : dbType;

  // Form input change handler
  const handleChange = (e) => {
    setSourceDetails({ ...sourceDetails, [e.target.name]: e.target.value });
    setConnectionTested(false);
    setError("");
    setSuccessMsg("");
  };

  // Field validation logic
  const validateFields = () => {
    const requiredFields = ["host", "port", "username", "password"];
    if (currentDbType === "Oracle") {
      if (oracleConnectionType === "sid") requiredFields.push("sid");
      else requiredFields.push("service_name");
    } else {
      requiredFields.push("database");
    }
    return requiredFields.every((field) => sourceDetails[field]);
  };

  // Test connection action
  const testConnection = async () => {
    if (!validateFields()) {
      setError("Please fill all required fields before testing connection.");
      setSuccessMsg("");
      setConnectionTested(false);
      return false;
    }
    setLoading(true);
    const payload = {
      db_type: currentDbType.toLowerCase(),
      ...sourceDetails,
      connection_type: oracleConnectionType,
    };
    try {
      const response = await fetch("https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (data.status === "success") {
        setSuccessMsg("Connection Successful!");
        setError("");
        setConnectionTested(true);
        setLoading(false);
        return true;
      } else {
        setError(data.message || "❌ Connection Failed");
        setSuccessMsg("");
        setConnectionTested(false);
        setLoading(false);
        return false;
      }
    } catch (err) {
      setError("❌ Error: " + err.message);
      setSuccessMsg("");
      setConnectionTested(false);
      setLoading(false);
      return false;
    }
  };

  // Cancel handler
  const handleCancel = () => {
    setSourceDetails({ ...DEFAULT_DETAILS });
    setShowHelloMessage(false);
    onClose();
  };

  // Predict or Next handler
  const handlePredictOrNext = () => {
    if (!connectionTested) {
      setError(
        isAIMode
          ? "Please test the connection before predicting."
          : "Please test the connection before proceeding."
      );
      return;
    }
    if (isAIMode && !showHelloMessage) {
      setShowHelloMessage(true);
    } else {
      onNext(currentDbType);
    }
  };

  if (!open) return null;

  return (
    <Modal
      open={open}
      passiveModal
      modalHeading={
        showHelloMessage ? "Prediction Result" : "Source Conversion Connection"
      }
      onRequestClose={handleCancel}
    >
      <Grid className="source-form-grid">
        <Column sm={4} md={8} lg={16}>
          {showHelloMessage ? (
            <ComplexityResult
              dbType={currentDbType}
              schema={
                currentDbType === "Oracle"
                  ? sourceDetails.service_name || sourceDetails.sid
                  : sourceDetails.database
              }
              onClose={handleCancel}
            />
          ) : (
            <>
              {/* DB Type Selection */}
              {!disableDbTypeSelect ? (
                <Select
                  id="db-type"
                  labelText="Select Source Database"
                  value={dbType}
                  onChange={(e) => {
                    setDbType(e.target.value);
                    setConnectionTested(false);
                    setError("");
                    setSuccessMsg("");
                  }}
                  className="full-width"
                >
                  <SelectItem text="Oracle" value="Oracle" />
                  <SelectItem text="SQL Server" value="SQL" />
                </Select>
              ) : (
                <TextInput
                  labelText="Source Database"
                  value={currentDbType}
                  readOnly
                  className="full-width"
                />
              )}

              {/* Common fields */}
              <TextInput
                labelText="Host"
                name="host"
                value={sourceDetails.host}
                onChange={handleChange}
                className="full-width"
              />
              <TextInput
                labelText="Port"
                name="port"
                value={sourceDetails.port}
                onChange={handleChange}
                className="full-width"
              />
              <TextInput
                labelText="Username"
                name="username"
                value={sourceDetails.username}
                onChange={handleChange}
                className="full-width"
              />
              <TextInput
                labelText="Password"
                type="password"
                name="password"
                value={sourceDetails.password}
                onChange={handleChange}
                className="full-width"
              />

              {/* Oracle fields */}
              {currentDbType === "Oracle" ? (
                <>
                  <div className="oracle-connection-type" style={{ margin: "18px 0 2px 0" }}>
                    <RadioButtonGroup
                      legendText="Oracle Connection Type"
                      name="oracle-connection-type"
                      value={oracleConnectionType}
                      orientation="horizontal"
                      onChange={(val) => {
                        setOracleConnectionType(val);
                        setConnectionTested(false);
                        setError("");
                        setSuccessMsg("");
                      }}
                    >
                      <RadioButton
                        labelText="Service Name"
                        value="service_name"
                        id="service_name"
                      />
                      <RadioButton labelText="SID" value="sid" id="sid" />
                    </RadioButtonGroup>
                  </div>
                  {oracleConnectionType === "sid" ? (
                    <TextInput
                      labelText="SID"
                      name="sid"
                      value={sourceDetails.sid}
                      onChange={handleChange}
                      className="full-width"
                    />
                  ) : (
                    <TextInput
                      labelText="Service Name"
                      name="service_name"
                      value={sourceDetails.service_name}
                      onChange={handleChange}
                      className="full-width"
                    />
                  )}
                </>
              ) : (
                // SQL Server / Other field
                <TextInput
                  labelText="Database"
                  name="database"
                  value={sourceDetails.database}
                  onChange={handleChange}
                  className="full-width"
                />
              )}

              <div className="test-connection-wrapper">
                <Button
                  kind="tertiary"
                  onClick={testConnection}
                  className="test-connection-btn"
                  disabled={loading}
                >
                  {loading ? "Testing..." : "Test Connection"}
                </Button>
              </div>

              {(error || successMsg) && (
                <InlineNotification
                  kind={error ? "error" : "success"}
                  title={error ? "Error" : "Success"}
                  subtitle={error || successMsg}
                  onCloseButtonClick={() => {
                    setError("");
                    setSuccessMsg("");
                  }}
                  className="full-width"
                />
              )}

              <div className="footer-action-buttons" style={{ marginTop: 16 }}>
                <Button
                  kind="secondary"
                  onClick={handleCancel}
                  className="footer-btn cancel-btn"
                  disabled={loading}
                >
                  Cancel
                </Button>
                <Button
                  kind="primary"
                  onClick={handlePredictOrNext}
                  className="footer-btn next-btn"
                  disabled={!connectionTested || loading}
                >
                  {isAIMode ? "Compute Complexity" : "Next"}
                </Button>
              </div>
            </>
          )}
        </Column>
      </Grid>
    </Modal>
  );
};

export default SourceConnection;
