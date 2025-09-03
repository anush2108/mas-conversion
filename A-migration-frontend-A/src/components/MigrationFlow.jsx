import React from "react";
import "../styles/MigrationFlow.css";
import {
  Rocket,
  TableSplit,
  View,
  Events,
  DataBase,
  ChartBar,
} from "@carbon/icons-react";

const steps = [
  { label: "Table Creation", Icon: TableSplit, description: "Create target tables in DB2" },
  { label: "Data Load", Icon: DataBase, description: "Load table data" },
  { label: "Sequence Conversion", Icon: Rocket, description: "Convert Oracle sequences to DB2" },
  
  { label: "Trigger Conversion", Icon: Events, description: "Convert Oracle triggers to DB2" },
  { label: "Index Conversion", Icon: ChartBar, description: "Recreate indexes in DB2" },
  { label: "View Conversion", Icon: View, description: "Convert Oracle views" },
];

const MigrationFlow = () => {
  return (
    <div className="migration-container">
      <h2 className="flow-title">Conversion <span className="highlight-blue">Workflow</span></h2>
      <p className="flow-subtitle">Follow each step of the database Conversion journey</p>
      <div className="flow-line" />
      <div className="flow-steps">
        {steps.map(({ label, Icon, description }, index) => (
          <div key={index} className="flow-step">
            <div className="icon-circle">
              <Icon size={32} />
            </div>
            <div className="step-content">
              <h4>{label}</h4>
              <p>{description}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default MigrationFlow;
