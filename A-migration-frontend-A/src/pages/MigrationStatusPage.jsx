import React, { useEffect, useState } from "react";
import { InlineNotification } from "@carbon/react";
import axios from "axios";
import { useLocation, useNavigate } from "react-router-dom";
import MigrationStatusModal from "../components/MigrationStatusModal";

const POLL_INTERVAL = 10000;

const MigrationStatusPage = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const passedMigration = location.state?.migration;
  const [currentMigration, setCurrentMigration] = useState(passedMigration || null);
  const [statusData, setStatusData] = useState(null);
  const [error, setError] = useState(null);

  // If not passed from sidebar, try to load from API
  useEffect(() => {
    if (!currentMigration) {
      axios
        .get("https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/current-migration")
        .then((res) => setCurrentMigration(res.data))
        .catch((err) => {
          setError(err.response?.data?.detail || "No Conversion running");
          setTimeout(() => navigate("/dashboard"), 1500);
        });
    }
  }, [currentMigration, navigate]);

  // Poll for status
  useEffect(() => {
    if (!currentMigration) return;

    const fetchStatus = () => {
      axios
        .get(`https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/migration-status/${currentMigration.transaction_id}`, {
          params: {
            source_type: currentMigration.source_type,
            schema: currentMigration.schema,
            prefer_maximo_meta :true
          }
        })
        .then((res) => setStatusData(res.data))
        .catch((err) => setError(err.message));
    };

    fetchStatus();
    const interval = setInterval(fetchStatus, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [currentMigration]);

  if (error) return <InlineNotification kind="error" title="Error" subtitle={error} />;

  const objectsProgress = statusData
    ? Object.entries(statusData.done_counts).map(([type, data]) => ({
        type,
        success: data.success_count,
        total: data.total,
        errors: data.errors
      }))
    : [];

  return (
    <MigrationStatusModal
      open={true}
      onClose={() => navigate("/dashboard")}
      migrating={true}
      title={`Conversion Progress — ${currentMigration?.schema || "Unknown"}`}
      progressValue={statusData?.overall?.percentage || 0}
      progressMax={100}
      progressLabel={
        statusData ? `${statusData.overall.percentage}% Completed` : "Loading..."
      }
      objectsProgress={objectsProgress}
      logs={[]}
      disableClose={false}
      inlineNotification={{
        kind: "info",
        title: "Live Progress",
        subtitle: `Last updated: ${new Date().toLocaleTimeString()}`,
        hideCloseButton: true
      }}
    />
  );
};

export default MigrationStatusPage;
