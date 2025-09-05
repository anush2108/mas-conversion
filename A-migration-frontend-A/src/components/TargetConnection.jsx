import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Modal,
  Grid,
  Column,
  TextInput,
  Button,
  InlineNotification,
} from "@carbon/react";
import { saveCredentials } from "../utils/dbCredentials";
import "../styles/TargetConnection.css";

const TargetConnection = ({ open, onClose, onBack, sourceDb, sourceConnection }) => {
  const navigate = useNavigate();
  const [targetDetails, setTargetDetails] = useState({
    host: "",
    port: "",
    username: "",
    password: "",
    database: "",
    security: "",
  });

  const [error, setError] = useState("");
  const [successMsg, setSuccessMsg] = useState("");
  const [canMigrate, setCanMigrate] = useState(false);

  const handleChange = (e) => {
    setTargetDetails({ ...targetDetails, [e.target.name]: e.target.value });
    setError("");
    setSuccessMsg("");
    setCanMigrate(false);
  };

  const validateFields = () =>
    Object.values(targetDetails).every((val) => val.trim() !== "");

  const testConnection = async () => {
    if (!validateFields()) {
      setError("Please fill in all required fields.");
      return;
    }
    try {
      const response = await fetch("https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/test-connection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ db_type: "db2", ...targetDetails }),
      });
      const data = await response.json();
      if (data.status === "success") {
        setSuccessMsg("Connection to target Db2 successful.");
        setCanMigrate(true);
        saveCredentials("target", { db_type: "db2", ...targetDetails });
      } else {
        setError(data.message || "Connection failed.");
        setCanMigrate(false);
      }
    } catch (err) {
      setError("Connection Test Failed: " + err.message);
      setCanMigrate(false);
    }
  };

  const migrateData = () => {
    if (!validateFields())
      return setError("Fill in all fields before converting.");
    if (!canMigrate) return setError("Test connection before converting.");

    onClose();

    // Case-insensitive check for Embedded SQL Conversion
    if (
      typeof sourceDb === "string" &&
      sourceDb.trim().toLowerCase() === "embedded sql conversion"
    ) {
      // Pass both source and target connection details to the new page
      navigate("/embedded-sql-migration", {
        state: {
          sourceDbType: sourceDb,
          sourceConnection: sourceConnection || null,
          targetConnection: targetDetails,
        },
      });
    } else {
      navigate("/full-schema-migration", {
        state: {
          sourceDbType: sourceDb,
          sourceConnection: sourceConnection || null,
          targetConnection: targetDetails,
        },
      });
    }
  };

  const handleCancel = () => {
    setTargetDetails({
      host: "",
      port: "",
      username: "",
      password: "",
      database: "",
      security: "",
    });
    setError("");
    setSuccessMsg("");
    setCanMigrate(false);
    onClose();
  };

  return (
    <Modal
      open={open}
      modalHeading={`Target Conversion Connection`}
      passiveModal
      onRequestClose={handleCancel}
    >
      <Grid className="target-grid">
        <Column lg={16} md={8} sm={4}>
          <TextInput
            labelText="Host"
            name="host"
            value={targetDetails.host}
            onChange={handleChange}
          />
          <TextInput
            labelText="Port"
            name="port"
            value={targetDetails.port}
            onChange={handleChange}
          />
          <TextInput
            labelText="Username"
            name="username"
            value={targetDetails.username}
            onChange={handleChange}
          />
          <TextInput
            labelText="Password"
            type="password"
            name="password"
            value={targetDetails.password}
            onChange={handleChange}
          />
          <TextInput
            labelText="Database"
            name="database"
            value={targetDetails.database}
            onChange={handleChange}
          />
          <TextInput
            labelText="Security"
            name="security"
            value={targetDetails.security}
            onChange={handleChange}
          />

          <Button kind="tertiary" onClick={testConnection} className="test-btn">
            Test Connection
          </Button>

          {(error || successMsg) && (
            <InlineNotification
              kind={error ? "error" : "success"}
              title={error ? "Error" : "Success"}
              subtitle={error || successMsg}
              onCloseButtonClick={() => {
                setError("");
                setSuccessMsg("");
              }}
              className="notification-box"
            />
          )}

          <div className="target-btn-group">
            <Button
              kind="secondary"
              onClick={onBack || handleCancel}
              className="target-btn back-btn"
            >
              Back
            </Button>
            <Button
              kind="primary"
              onClick={migrateData}
              className="target-btn migrate-btn"
              disabled={!canMigrate}
            >
              Convert
            </Button>
          </div>
        </Column>
      </Grid>
    </Modal>
  );
};

export default TargetConnection;
