// src/components/GlobalMigrationStatusModal.jsx
import React, { useEffect, useState } from "react";
import axios from "axios";
import MigrationStatusModal from "./MigrationStatusModal";
import { useMigrationContext } from "../context/MigrationContext";

const POLL_INTERVAL = 1000; // 1 seconds

const GlobalMigrationStatusModal = () => {
  const { modalOpen, setModalOpen, currentMigration } = useMigrationContext();
  const [statusData, setStatusData] = useState(null);

  useEffect(() => {
    setStatusData(null);

    if (!modalOpen || !currentMigration) return;

    let isMounted = true;
    const activeTransactionId = currentMigration.transaction_id;
    let interval;

    const fetchStatus = async () => {
      try {
        const res = await axios.get(
          `http://localhost:8000/migration-status/${activeTransactionId}`,
          {
            params: {
              source_type: currentMigration.source_type,
              schema: currentMigration.schema,
              prefer_maximo_meta: true,
            },
          }
        );

        // 🛑 Ignore responses from previous migrations
        if (isMounted && activeTransactionId === currentMigration.transaction_id) {
          setStatusData(res.data);
        }
      } catch (e) {
        console.error("Failed to fetch migration status", e);
      }
    };

    fetchStatus();
    interval = setInterval(fetchStatus, POLL_INTERVAL);

    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, [modalOpen, currentMigration]);

  const objectsProgress = statusData
    ? Object.entries(statusData.done_counts).map(([type, data]) => ({
        type,
        success: data.success_count,
        total: data.total,
        errors: data.errors,
      }))
    : [];

  return (
    <MigrationStatusModal
      open={modalOpen}
      onClose={() => setModalOpen(false)}
      migrating={!!currentMigration}
      title={`Migration Progress — ${currentMigration?.schema || "Unknown"}`}
      progressValue={statusData?.overall?.percentage || 0}
      progressMax={100}
      progressLabel={
        statusData
          ? `${statusData.overall.percentage}% Completed`
          : "Loading..."
      }
      objectsProgress={objectsProgress}
      logs={[]}
      disableClose={false}
      inlineNotification={
        statusData
          ? {
              kind: "info",
              title: "Live Progress",
              subtitle: `Last updated: ${new Date().toLocaleTimeString()}`,
              hideCloseButton: true,
            }
          : null
      }
    />
  );
};

export default GlobalMigrationStatusModal;
