import React, { useEffect, useState } from "react";
import { Search, InlineLoading } from "@carbon/react";
import "../styles/SchemaSelector.css";

const SchemaSelector = ({ sourceDbType, selectedSchema, onSchemaSelect }) => {
  const [schemas, setSchemas] = useState([]);
  const [filteredSchemas, setFilteredSchemas] = useState([]);
  const [schemaSearch, setSchemaSearch] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!sourceDbType) return;
    setLoading(true);
    setSchemas([]);
    setFilteredSchemas([]);

    const apiPath = sourceDbType.toLowerCase() === "oracle" ? "/o_schemas" : "/s_schemas";

    fetch(`https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com${apiPath}`)
      .then((res) => res.json())
      .then((data) => {
        setSchemas(data);
        setFilteredSchemas(data);
      })
      .catch((err) => {
        console.error("Failed to fetch schemas:", err);
        setSchemas([]);
        setFilteredSchemas([]);
      })
      .finally(() => setLoading(false));
  }, [sourceDbType]);

  useEffect(() => {
    const filtered = schemas.filter((schema) =>
      schema.toLowerCase().includes(schemaSearch.toLowerCase())
    );
    setFilteredSchemas(filtered);
  }, [schemaSearch, schemas]);

  return (
    <div className="schema-selector-container">
      <h4 className="schema-title">Schemas</h4>
      <Search
        placeholder="Search schemas..."
        labelText=""
        size="lg"
        value={schemaSearch}
        onChange={(e) => setSchemaSearch(e.target.value)}
        className="schema-search"
      />

      <div className="schema-header">
        <div className="select-header">Select</div>
        <div className="name-header">Name</div>
      </div>

      <div className="schema-list">
        {loading ? (
          <InlineLoading description="Fetching schemas..." />
        ) : (
          filteredSchemas.map((schema) => (
            <div
              key={schema}
              className={`schema-item ${selectedSchema === schema ? "active" : ""}`}
              onClick={() => onSchemaSelect(schema)}
            >
              <input
                type="checkbox"
                checked={selectedSchema === schema}
                readOnly
              />
              <span className="schema-name">{schema}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default SchemaSelector;
