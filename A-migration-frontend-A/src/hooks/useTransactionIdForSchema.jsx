// src/hooks/useTransactionIdForSchema.jsx

import { useState, useEffect } from "react";
import db from "../components/cdbConnection";
import { v4 as uuidv4 } from "uuid";

export function useTransactionIdForSchema(schema) {
  const [transactionId, setTransactionId] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!schema) {
      setTransactionId(null);
      setError(null);
      setLoading(false);
      return;
    }

    let isMounted = true;
    const docId = `schema_${schema}`;

    async function fetchOrCreateTransactionId() {
      setLoading(true);
      setError(null);
      try {
        const doc = await db.get(docId);
        if (isMounted) {
          if (doc.transactionId) {
            setTransactionId(doc.transactionId);
          } else {
            const newId = uuidv4();
            await db.put({ _id: docId, transactionId: newId });
            setTransactionId(newId);
          }
        }
      } catch (err) {
        if (isMounted) {
          if (err.status === 404) {
            const newId = uuidv4();
            try {
              await db.put({ _id: docId, transactionId: newId });
              setTransactionId(newId);
            } catch (putErr) {
              setError(putErr);
              setTransactionId(null);
            }
          } else {
            setError(err);
            setTransactionId(null);
          }
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    }

    fetchOrCreateTransactionId();

    return () => {
      isMounted = false;
    };
  }, [schema]);

  return { transactionId, loading, error };
}
