// src/pages/LandingPage.jsx
import React, { useRef, useState, useEffect } from "react";
import { Grid, Column, Tile, Button } from "@carbon/react";
import "../styles/LandingPage.css";
import "../styles/Hero_Section.css";

import Header from "../components/Header";
import Footer from "../components/Footer";
import HeroSection from "./HeroSection";  // Correct PascalCase import
import SourceConnection from "../components/SourceConnection";
import TargetConnection from "../components/TargetConnection";
import MigrationFlow from "../components/MigrationFlow";

const migrationOptions = [
  {
    label: "Oracle/MS SQL to DB2 Conversion",
  image: "/oracle.png",
  description:
    "Migrate from Oracle and MS SQL Server to IBM Db2 with automated PL/SQL and T-SQL conversion, procedure mapping.",
  features: [
     "PL/SQL and T-SQL Conversion",
    "Schema and Stored Procedure Mapping",
    "Performance Optimization and Data Validation",
    "Zero Downtime and Security Conversion",
    ],
  },
  {
    label: "AI Data Conversion Complexity Predictor",
    image: "/AI.png",
    description:
      "Predicts the complexity level of data conversion project—Low, Medium, or High—based on various technical parameters using a machine learning model.",
    features: [
      "Automatic complexity evaluation from source credentials",
      
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

const useCases = [
  {
    title: "Accelerating Datalake table queries using MQTs in Db2 Warehouse",
    description:
      "Learn how columnar MQTs accelerate queries against Datalake tables by up to 10x performance improvement.",
    image:
      "https://www.ibm.com/content/dam/connectedassets-adobe-cms/worldwide-content/creative-assets/s-migr/ul/g/f9/eb/cics-transaction-server-leadspace.png",
  },
  {
    title: "The Neural Networks Powering the Db2 AI Query Optimizer",
    description:
      "Discover how neural networks enhance Db2 query speed and accuracy through intelligent optimization algorithms.",
    image:
      "https://prolifics.com/uk/wp-content/uploads/2023/09/Netezza-Hero.webp",
  },
  {
    title: "Introducing IBM's Power10 Private Cloud Rack for Db2 Warehouse Solutions",
    description:
      "Explore the architecture & performance of IBM's next-gen data warehouse solution built on Power10 infrastructure.",
    image:
      "https://www.ibm.com/content/dam/connectedassets-adobe-cms/worldwide-content/creative-assets/s-migr/ul/g/f9/eb/cics-transaction-server-leadspace.png",
  },
  {
    title: "pureScale with Pacemaker – Chapter 2: Have we reached quorum?",
    description:
      "Learn about the concept of quorum and fencing in a pureScale cluster for maximum uptime and availability.",
    image:
      "https://media.licdn.com/dms/image/sync/v2/D5627AQEv2u7tS0DJVA/articleshare-shrink_800/articleshare-shrink_800/0/1734324905068?e=2147483647&v=beta&t=wxMHTzrDKuE3e5oSV-fCwNpx-jh6ORaMtBIp-ShREj8",
  },
];

const LandingPage = () => {
  const [isBuildModalOpen, setIsBuildModalOpen] = useState(false);
  const [isTargetOpen, setIsTargetOpen] = useState(false);
  const [sourceDbType, setSourceDbType] = useState("Oracle");
  const [disableDbSelect, setDisableDbSelect] = useState(false);
  const [isAIMode, setIsAIMode] = useState(false);
  const [showHelloWorld, setShowHelloWorld] = useState(false);
  const optionsRef = useRef(null);

  const [isLoggedIn, setIsLoggedIn] = useState(false);

  useEffect(() => {
    const checkLogin = async () => {
      try {
        const res = await fetch("https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/auth/account", {
          credentials: "include",
        });
        if (res.ok) {
          const data = await res.json();
          setIsLoggedIn(!!data.email);
        } else {
          setIsLoggedIn(false);
        }
      } catch {
        setIsLoggedIn(false);
      }
    };
    checkLogin();
  }, []);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.style.animationDelay = Math.random() * 0.3 + "s";
            entry.target.classList.add("fade-in-up");
          }
        });
      },
      { threshold: 0.1, rootMargin: "0px 0px -50px 0px" }
    );

    document.querySelectorAll(".fade-in-up").forEach((el) => observer.observe(el));
    return () => observer.disconnect();
  }, []);

  const handleSourceNext = (dbType) => {
    setIsBuildModalOpen(false);
    setSourceDbType(dbType);
    if (isAIMode) {
      setShowHelloWorld(true);
    } else {
      setIsTargetOpen(true);
    }
  };

  const handleCloseHelloWorld = () => {
    setShowHelloWorld(false);
  };

  return (
    <div style={{ display: "flex" }}>
      {/* Sidebar (if any) */}

      <div className="landing-page" style={{ flexGrow: 1 }}>
        <Header setIsLoggedIn={setIsLoggedIn} />

        <HeroSection
          setDisableDbSelect={setDisableDbSelect}
          setIsBuildModalOpen={setIsBuildModalOpen}
          setIsAIMode={setIsAIMode} // Pass handler to HeroSection
          isLoggedIn={isLoggedIn}
        />

        {/* Modals */}
        <SourceConnection
          open={isBuildModalOpen}
          onClose={() => setIsBuildModalOpen(false)}
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
            setIsBuildModalOpen(true);
          }}
        />

        <MigrationFlow />

        <section className="landing-options" ref={optionsRef}>
          <div className="container">
            <Grid>
              <Column lg={16} md={8} sm={4}>
                <h2 className="section-heading">
                  Conversion <span className="highlight-blue">Pathways</span>
                </h2>
                <p className="section-description">
                  Choose your source database and learn about the intelligent conversion engine behind each offering.
                </p>
              </Column>

              <Column lg={16} md={8} sm={4}>
                <div className="migration-card-container">
                  {migrationOptions.map(({ label, image, description, features }, index) => {
                    let cardClass = "option-tile fade-in-up";
                    if (index === 1) cardClass += " move-up";
                    if (index === 2) cardClass += " move-down";

                    return (
                      <Tile key={label} className={cardClass}>
                        <div className="option-tile-inner">
                          <div className="product-header">
                            <div className="option-icon-wrapper">
                              <img src={image} alt={`${label} Logo`} className="option-image" loading="lazy" />
                            </div>
                            <h3 className="option-title">{label}</h3>
                          </div>
                          <p className="option-description">{description}</p>
                          <div className="option-features">
                            <h4>Key Features:</h4>
                            <ul>
                              {features.map((feature, i) => <li key={i}>{feature}</li>)}
                            </ul>
                          </div>
                        </div>
                      </Tile>
                    );
                  })}
                </div>
              </Column>
            </Grid>
          </div>
        </section>

        <section className="landing-usecases">
          <div className="container">
            <Grid>
              <Column lg={16} md={8} sm={4}>
                <h2 className="section-heading">
                  Success Stories <span className="highlight-blue">& Use Cases</span>
                </h2>
                <p className="section-description">
                  Learn from real-world implementations and best practices
                </p>
              </Column>

              {useCases.map(({ title, description, image }, index) => (
                <Column key={index} lg={4} md={4} sm={4}>
                  <Tile className="usecase-tile fade-in-up">
                    <div className="usecase-image-wrapper">
                      <img src={image} alt={title} className="usecase-image" loading="lazy" />
                    </div>
                    <div className="usecase-content">
                      <h3 className="usecase-title">{title}</h3>
                      <p className="usecase-description">{description}</p>
                    </div>
                  </Tile>
                </Column>
              ))}
            </Grid>
          </div>
        </section>

        {/* Show Hello World message inline for AI prediction */}
        {showHelloWorld && (
          <div className="hello-world-page" style={{ padding: 20, textAlign: "center" }}>
            <h2>Hello World</h2>
            <Button onClick={handleCloseHelloWorld}>Close</Button>
          </div>
        )}

        <Footer />
      </div>
    </div>
  );
};

export default LandingPage;
