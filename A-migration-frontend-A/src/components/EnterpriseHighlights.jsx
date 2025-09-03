import React from "react";
import { Grid, Column } from "@carbon/react";
import {
  Analytics,
  AppConnectivity,
  Cloud,
  DataBase,
} from "@carbon/icons-react";
import "../styles/EnterpriseHighlights.css";
import { motion } from "framer-motion";

const features = [
  {
    icon: Analytics,
    title: "Smart Analytics",
    description:
      "Leverage AI-driven insights to optimize database migration and reduce overhead.",
    bgColor: "#0f62fe",
  },
  {
    icon: AppConnectivity,
    title: "Seamless Integration",
    description:
      "Easily integrate with enterprise systems, cloud platforms, and legacy databases.",
    bgColor: "#0f62fe",
  },
  {
    icon: Cloud,
    title: "Cloud Ready",
    description:
      "Migrate and modernize with support for hybrid and multi-cloud architectures.",
    bgColor: "#0f62fe",
  },
  {
    icon: DataBase,
    title: "Reliable Performance",
    description:
      "Ensure zero downtime and guaranteed data integrity across environments.",
    bgColor: "#0f62fe",
  },
];

const EnterpriseHighlights = () => {
  return (
    <section className="enterprise-highlights-section">
      <motion.div
        initial={{ opacity: 0, y: 40 }}
        whileInView={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
        className="highlights-title-area"
      >
        <h2> Why Enterprises <span className="highlight-blue">Choose Us</span></h2>
        <p>
          Empowering organizations with seamless database transformation, industry-grade
          compatibility, and intelligent migration flows.
        </p>
      </motion.div>

      <Grid className="highlight-cards-grid">
        {features.map(({ icon: Icon, title, description, bgColor }, idx) => (
          <Column key={idx} sm={4} md={2} lg={4} className="highlight-card">
            <motion.div
              initial={{ opacity: 0, y: 30 }}
              whileInView={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: idx * 0.2 }}
            >
              <div
                className="highlight-icon-wrapper"
                style={{ backgroundColor: bgColor }}
              >
                <Icon size={32} className="highlight-icon" />
              </div>
              <h4 className="highlight-title">{title}</h4>
              <p className="highlight-desc">{description}</p>
            </motion.div>
          </Column>
        ))}
      </Grid>
    </section>
  );
};

export default EnterpriseHighlights;
