import { useState, useEffect, useCallback } from "react";
import axios from "axios";

/**
 * Polls the backend /migration-status API for the given transactionId.
 * Returns progress data, loading/error states, and a manual refresh function.
 */
export function useMigrationStatus(transactionId, sourceType, schema) {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchStatus = useCallback(async () => {
    if (!transactionId || !sourceType || !schema) return;

    try {
      setLoading(true);
      const res = await axios.get(
        `https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/migration-status/${transactionId}`,
        {
          params: {
            source_type: sourceType,
            schema,
            prefer_maximo_meta: true
          }
        }
      );
      setStatus(res.data);
      setError(null);
    } catch (err) {
      setError(err.message || "Failed to fetch migration status");
    } finally {
      setLoading(false);
    }
  }, [transactionId, sourceType, schema]);

  // 🌀 Poll every 15 seconds while modal is open
  useEffect(() => {
    if (!transactionId || !sourceType || !schema) return;

    fetchStatus(); // initial fetch
    const interval = setInterval(fetchStatus, 15000);

    return () => clearInterval(interval);
  }, [transactionId, sourceType, schema, fetchStatus]);

  return { status, loading, error, refresh: fetchStatus };
}
