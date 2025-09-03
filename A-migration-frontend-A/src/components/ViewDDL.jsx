import React, { useState, useEffect } from "react";
import {
  Button,
  Modal,
  ModalBody,
  ModalFooter,
  Select,
  SelectItem,
  InlineNotification,
  CodeSnippet,
} from "@carbon/react";

const BackendBaseUrl = "https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com";

const objectTypes = [
  { key: "table", text: "Tables" },
  { key: "view", text: "Views" },
  { key: "sequence", text: "Sequences" },
  { key: "trigger", text: "Triggers" },
  { key: "index", text: "Indexes" },
];

const ViewDDL = ({ open, onClose }) => {
  const [targets, setTargets] = useState([]);
  const [target, setTarget] = useState("");
  const [schemas, setSchemas] = useState([]);
  const [selectedSchema, setSelectedSchema] = useState("");
  const [objectType, setObjectType] = useState("");
  const [objects, setObjects] = useState([]);
  const [selectedObject, setSelectedObject] = useState("");
  const [ddlContent, setDdlContent] = useState("");
  const [loadingTargets, setLoadingTargets] = useState(false);
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingObjects, setLoadingObjects] = useState(false);
  const [loadingDdl, setLoadingDdl] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (open) {
      setTargets([]);
      setTarget("");
      setSchemas([]);
      setSelectedSchema("");
      setObjectType("");
      setObjects([]);
      setSelectedObject("");
      setDdlContent("");
      setError(null);
      setLoadingTargets(false);
      setLoadingSchemas(false);
      setLoadingObjects(false);
      setLoadingDdl(false);
      fetchTargets();
    }
    // eslint-disable-next-line
  }, [open]);

  useEffect(() => {
    if (target) {
      setSchemas([]);
      setSelectedSchema("");
      setObjectType("");
      setObjects([]);
      setSelectedObject("");
      setDdlContent("");
      setError(null);
      fetchSchemas(target);
    }
  }, [target]);

  useEffect(() => {
    if (target && selectedSchema && objectType) {
      setObjects([]);
      setSelectedObject("");
      setDdlContent("");
      setError(null);
      fetchObjects(target, selectedSchema, objectType);
    }
  }, [target, selectedSchema, objectType]);

  async function fetchTargets() {
    setLoadingTargets(true);
    setError(null);
    try {
      const res = await fetch(`${BackendBaseUrl}/ddl/targets`);
      if (!res.ok) throw new Error("Failed to fetch targets");
      const data = await res.json();
      setTargets(data || []);
      if (data && data.length > 0) setTarget(data[0]);
    } catch (e) {
      setError(`Targets Error: ${e.message}`);
    }
    setLoadingTargets(false);
  }

  async function fetchSchemas(selectedTarget) {
    setLoadingSchemas(true);
    setError(null);
    try {
      const res = await fetch(
        `${BackendBaseUrl}/ddl/schemas?target=${encodeURIComponent(selectedTarget)}`
      );
      if (!res.ok) {
        const errJson = await res.json();
        throw new Error(errJson.detail || "Failed to fetch schemas");
      }
      const data = await res.json();
      setSchemas(data.schemas || []);
    } catch (e) {
      setError(`Schemas Error: ${e.message}`);
    }
    setLoadingSchemas(false);
  }

  const normalizeObjectType = (type) => {
    switch (type) {
      case "tables": return "table";
      case "views": return "view";
      case "sequences": return "sequence";
      case "triggers": return "trigger";
      case "indexes": return "index";
      default: return type;
    }
  };

  async function fetchObjects(selectedTarget, schema, objectType) {
    setLoadingObjects(true);
    setError(null);
    try {
      const normalizedType = normalizeObjectType(objectType);
      const res = await fetch(
        `${BackendBaseUrl}/ddl/objects?target=${encodeURIComponent(
          selectedTarget
        )}&schema=${encodeURIComponent(schema)}&object_type=${encodeURIComponent(
          normalizedType
        )}`
      );
      if (!res.ok) {
        const errJson = await res.json();
        throw new Error(errJson.detail || "Failed to fetch objects");
      }
      const data = await res.json();
      setObjects(data.objects || []);
    } catch (e) {
      setError(`Objects Error: ${e.message}`);
    }
    setLoadingObjects(false);
  }

  async function fetchDdl() {
    setLoadingDdl(true);
    setError(null);
    setDdlContent("");
    if (!target || !selectedSchema || !objectType || !selectedObject) {
      setError("Please select all fields.");
      setLoadingDdl(false);
      return;
    }
    try {
      const normalizedType = normalizeObjectType(objectType);
      const url = `${BackendBaseUrl}/ddl/object_ddl?target=${encodeURIComponent(
        target
      )}&schema=${encodeURIComponent(
        selectedSchema
      )}&object_type=${encodeURIComponent(
        normalizedType
      )}&object_name=${encodeURIComponent(selectedObject)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const errJson = await res.json();
        throw new Error(errJson.detail || "Failed to fetch DDL");
      }
      const data = await res.json();
      setDdlContent(data.ddl || "-- No DDL available --");
    } catch (e) {
      setError(`DDL Error: ${e.message}`);
    }
    setLoadingDdl(false);
  }

  function downloadDdl() {
    if (!ddlContent) return;
    const filename = `${selectedSchema}_${selectedObject}_ddl.sql`;
    const blob = new Blob([ddlContent], { type: "text/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  return (
    <Modal
      open={open}
      modalHeading="View DDL"
      onRequestClose={onClose}
      size="lg"
      passiveModal={true}
    >
      <ModalBody>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <Select
            id="target-select"
            labelText="Target"
            disabled={loadingTargets || targets.length === 0}
            onChange={(e) => setTarget(e.target.value)}
            value={target}
          >
            <SelectItem value="" text="-- Select Target --" disabled />
            {targets.map((t) => (
              <SelectItem
                key={t}
                value={t}
                text={t.charAt(0).toUpperCase() + t.slice(1)}
              />
            ))}
          </Select>
          <Select
            id="schema-select"
            labelText="Schema"
            disabled={loadingSchemas || schemas.length === 0}
            onChange={(e) => setSelectedSchema(e.target.value)}
            value={selectedSchema}
          >
            <SelectItem value="" text="-- Select Schema --" disabled />
            {schemas.map((schema) => (
              <SelectItem key={schema} value={schema} text={schema} />
            ))}
          </Select>
          <Select
            id="object-type-select"
            labelText="Object Type"
            disabled={!selectedSchema}
            onChange={(e) => setObjectType(e.target.value)}
            value={objectType}
          >
            <SelectItem value="" text="-- Select Object Type --" disabled />
            {objectTypes.map(({ key, text }) => (
              <SelectItem key={key} value={key} text={text} />
            ))}
          </Select>
          <Select
            id="object-select"
            labelText="Object"
            disabled={!objectType || loadingObjects || objects.length === 0}
            onChange={(e) => setSelectedObject(e.target.value)}
            value={selectedObject}
          >
            <SelectItem value="" text="-- Select Object --" disabled />
            {objects.map((obj) => (
              <SelectItem key={obj} value={obj} text={obj} />
            ))}
          </Select>
          <Button
            onClick={fetchDdl}
            size="sm"
            disabled={loadingDdl}
            kind="primary"
            style={{ marginTop: 8, maxWidth: 150 }}
          >
            {loadingDdl ? "Fetching DDL..." : "Fetch DDL"}
          </Button>
        </div>

        {error && (
          <InlineNotification
            kind="error"
            title="Error"
            subtitle={error}
            style={{ margin: "18px 0 0 0" }}
            hideCloseButton
          />
        )}

        {ddlContent && !loadingDdl && !error && (
          <>
            <hr
              style={{
                border: "none",
                borderTop: "1px solid #e0e0e0",
                margin: "28px 0 12px 0",
                width: "100%",
              }}
            />
            <CodeSnippet
              type="multi"
              light
              style={{ maxHeight: 370, overflowY: "auto", fontSize: 13 }}
            >
              {ddlContent}
            </CodeSnippet>
          </>
        )}
      </ModalBody>
      <ModalFooter>
        {ddlContent && (
          <Button
            onClick={downloadDdl}
            size="sm"
            kind="secondary"
            style={{ marginRight: 0, marginLeft:-3  }}
          >
            Download DDL
          </Button>
        )}
        <Button onClick={onClose} size="sm" kind="primary">
          Close
        </Button>
      </ModalFooter>
    </Modal>
  );
};

export default ViewDDL;
