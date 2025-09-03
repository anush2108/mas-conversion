import React, { useState } from "react";
import { Button } from "@carbon/react";
import "../styles/ProductsPage.css";
import Header from "../components/Header";
import Footer from "../components/Footer";
import SourceConnection from "../components/SourceConnection";
import TargetConnection from "../components/TargetConnection";
import ComplexityResult from "../components/ComplexityResult";

const products = [
  {
    label: "Oracle",
    image: "/oracle.png",
    description:
      "Convert from Oracle to IBM Db2 with automated PL/SQL conversion, procedure mapping, and optimized performance tuning for enterprise environments.",
    features: [
      "PL/SQL Conversion",
      "Schema Mapping",
      "Performance Optimization",
      "Zero Downtime Conversion",
    ],
  },
  {
    label: "SQL",
    image:
      "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcST2gq0EJbNr7VkQ0GQ1yQ_Vs3sKZd9yQVP8g&s",
    description:
      "Seamlessly transition from SQL Server with T-SQL conversion, stored procedure conversion, and compatibility optimization for Db2.",
    features: [
      "T-SQL Conversion",
      "Stored Procedures",
      "Data Validation",
      "Security Conversion",
    ],
  },
  {
    label: "AI Data Conversion Complexity Predictor",
    image: "/AI.png",
    description:
      "Predict the complexity of data conversion projects (Low, Medium, High) using a machine learning model based on key technical parameters.",
    features: [
      "Structured questionnaire input",
      "AI-powered prediction of conversion complexity",
      "Early risk identification and mitigation planning support",
    ],
  },
  {
    label: "Embedded SQL Conversion",
    image: "https://www.freeiconspng.com/uploads/sql-server-icon-24.png", // Change to your real image path
    description:
      "Convert Embedded SQL statements in application code that are not compatible with Db2 using Watsonx AI for intelligent transformation.",
    features: [
      "Analyze application code for incompatible SQL",
      "AI-powered SQL rewriting using Watsonx",
      "Maintain functional equivalence",
      "Accelerate modernization projects",
    ],
  },
];

const ProductsPage = () => {
  const [isSourceOpen, setIsSourceOpen] = useState(false);
  const [isTargetOpen, setIsTargetOpen] = useState(false);
  const [sourceDbType, setSourceDbType] = useState(null);
  const [disableDbSelect, setDisableDbSelect] = useState(false);
  const [isAIMode, setIsAIMode] = useState(false);
  const [showComplexityResult, setShowComplexityResult] = useState(false);
  const [selectedSchema, setSelectedSchema] = useState(null);

  // Handles Oracle and SQL buttons (normal flow)
  const handleOracleOrSqlClick = (label) => {
    setSourceDbType(label);
    setDisableDbSelect(true);
    setIsAIMode(false);
    setIsSourceOpen(true);
    setShowComplexityResult(false);
  };

  // Handles AI Explore click
  const handleAIExploreClick = () => {
    setSourceDbType(null);
    setDisableDbSelect(false);
    setIsAIMode(true);
    setIsSourceOpen(true);
    setShowComplexityResult(false);
  };

  // Handles Embedded SQL Conversion button
const handleEmbeddedSQLClick = () => {
  setSourceDbType("Embedded SQL Conversion"); // <-- important!
  setDisableDbSelect(false);                  // allow Oracle/SQL choice
  setIsAIMode(false);
  setIsSourceOpen(true);
  setShowComplexityResult(false);
};





  // Called when SourceConnection's "Next" is clicked
  const handleSourceNext = (schema) => {
    if (schema) {
      setSelectedSchema(schema);
    }
    setIsSourceOpen(false);

    if (isAIMode) {
      setShowComplexityResult(true);
    } else {
      setIsTargetOpen(true);
    }
  };

  // Hide ComplexityResult view
  const handleCloseComplexityResult = () => {
    setShowComplexityResult(false);
  };

  return (
    <div className="landing-page">
      <Header />

      <section className="landing-options">
        <div className="container">
          <h2 className="section-heading">
            Our <span className="highlight-blue">Products</span>
          </h2>
          <p className="section-description">
            Explore our comprehensive set of conversion and validation offerings
          </p>

          <div className="products-grid">
            {products.map(({ label, image, description, features }) => (
              <div className="product-tile highlight-card" key={label}>
                <img
                  src={image}
                  alt={`${label} Logo`}
                  className="option-image"
                />
                <h3 className="option-title">{label}</h3>
                <p className="option-description">{description}</p>

                <div className="option-features">
                  <h4>Key Features:</h4>
                  <ul>
                    {features.map((feature, i) => (
                      <li key={i}>{feature}</li>
                    ))}
                  </ul>
                </div>

                <div className="option-button-wrapper">
                  {label === "Oracle" || label === "SQL" ? (
                    <Button
                      kind="primary"
                      onClick={() => handleOracleOrSqlClick(label)}
                      className="option-button"
                    >
                      {`Start with ${label}`}
                    </Button>
                  ) : label === "Embedded SQL Conversion" ? (
                    <Button
                      kind="primary"
                      className="option-button"
                      onClick={handleEmbeddedSQLClick}
                    >
                      Start Now
                    </Button>
                  ) : (
                    <Button
                      kind="primary"
                      className="option-button"
                      onClick={handleAIExploreClick}
                    >
                      Compute Complexity
                    </Button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <SourceConnection
        open={isSourceOpen}
        onClose={() => setIsSourceOpen(false)}
        onNext={handleSourceNext}
        sourceDbType={sourceDbType}
        disableDbTypeSelect={disableDbSelect}
        isAIMode={isAIMode}
      />

      <TargetConnection
        open={isTargetOpen}
        sourceDb={sourceDbType}
        onClose={() => setIsTargetOpen(false)}
        onBack={() => {
          setIsTargetOpen(false);
          setIsSourceOpen(true);
        }}
      />

      {/* Show ComplexityResult in AI mode */}
      {showComplexityResult && (
        <div style={{ padding: 20 }}>
          <ComplexityResult
            dbType={sourceDbType}
            schema={selectedSchema}
            onClose={handleCloseComplexityResult}
          />
        </div>
      )}

      <Footer />
    </div>
  );
};

export default ProductsPage;
