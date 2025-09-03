// src/components/MigrationStatusModal.jsx

import React from 'react';
import {
  ComposedModal,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ProgressBar,
  Button,
  InlineNotification,
} from '@carbon/react';

const defaultGetStyle = (type) => {
  switch (type) {
    case 'success': return { color: '#24a148', fontWeight: 500 };
    case 'error': return { color: '#da1e28', fontWeight: 500 };
    case 'warning': return { color: '#f1c21b', fontWeight: 500 };
    case 'info': return { color: '#0f62fe', fontWeight: 500 };
    default: return { color: '#525252' };
  }
};

const MigrationStatusModal = ({
  open,
  onClose,
  migrating,
  title = 'Conversion Progress',
  progressValue,
  progressMax,
  progressLabel,
  objectsProgress = [],
  logs = [],
  getLogStyle = defaultGetStyle,
  disableClose = false,
  extraHeaderContent = null,
  inlineNotification = null,
}) => (
  <ComposedModal
    open={open}
    onClose={onClose}
    size="lg"
    preventCloseOnEsc={disableClose && migrating}
    preventCloseOnClickOutside={disableClose && migrating}
  >
    <ModalHeader title={title}>
      {extraHeaderContent}
    </ModalHeader>
    <ModalBody>

      {typeof progressValue === 'number' && typeof progressMax === 'number' && (
        <div style={{ marginBottom: '1.5rem' }}>
          <ProgressBar
            value={progressValue}
            max={progressMax}
            label={progressLabel}
            size="lg"
          />
        </div>
      )}

      {objectsProgress.length > 0 && (
        <div style={{ marginBottom: '1.5rem' }}>
          {objectsProgress.map((obj) => (
            <div key={obj.type} style={{ marginBottom: '1rem' }}>
              <div style={{ fontWeight: 'bold', marginBottom: 4 }}>
                {obj.type} — {obj.success}/{obj.total}
              </div>
              <ProgressBar
                value={obj.success}
                max={obj.total || 1}
                label={`${obj.success}/${obj.total}`}
                size="lg"
              />
              {obj.errors && obj.errors.length > 0 && (
                <ul style={{ color: "#da1e28", marginTop: 4, marginLeft: 16 }}>
                  {obj.errors.map((err, idx) => <li key={idx}>{err}</li>)}
                </ul>
              )}
            </div>
          ))}
        </div>
      )}

      {inlineNotification && (
        <div style={{ marginBottom: '1rem' }}>
          <InlineNotification {...inlineNotification} />
        </div>
      )}

      <div style={{
        maxHeight: 400,
        overflowY: 'auto',
        fontFamily: 'monospace',
        fontSize: 13,
        backgroundColor: '#f4f4f4',
        padding: '1rem',
        borderRadius: 4,
        border: '1px solid #e0e0e0',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
      }}>
        {logs.length === 0 ? (
          <div style={{ color: '#888', fontStyle: 'italic' }}>No logs yet.</div>
        ) : (
          logs.map((log, idx) => (
            <div
              key={idx}
              style={{ ...getLogStyle(log.type), marginBottom: 6 }}
              title={log.message}
            >
              {log.message}
            </div>
          ))
        )}
      </div>
    </ModalBody>
    <ModalFooter>
      <Button onClick={onClose} disabled={disableClose && migrating}>Close</Button>
    </ModalFooter>
  </ComposedModal>
);

export default MigrationStatusModal;
