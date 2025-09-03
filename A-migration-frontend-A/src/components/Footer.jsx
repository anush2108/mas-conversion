// src/components/Footer.jsx
import React from "react";
import { Link } from "react-router-dom";
import { Grid, Column } from "@carbon/react";
import "../styles/LandingPage.css";

const Footer = () => (
  <footer className="landing-footer">
    <div className="container">
      <Grid>
        <Column lg={4} md={4} sm={4}>
          <Link to="/">
            <img src="/ibmm.webp" alt="IBM Logo" className="footer-logo" loading="lazy" />
          </Link>
          <p className="footer-copyright">
            © 2025 IBM Corporation. All rights reserved.
          </p>
        </Column>

        <Column lg={4} md={4} sm={4}>
          <h5 className="footer-heading">Conversion Services</h5>
          <ul className="footer-list">
            <li>Oracle to Db2</li>
            <li>SQL Server to Db2</li>
          </ul>
        </Column>

        <Column lg={4} md={4} sm={4}>
          <h5 className="footer-heading">Features</h5>
          <ul className="footer-list">
            <li>Schema Conversion</li>
            <li>Data Validation</li>
            <li>Performance Tuning</li>
            <li>Security Conversion</li>
          </ul>
        </Column>

        <Column lg={4} md={4} sm={4}>
          <h5 className="footer-heading">Resources</h5>
          <ul className="footer-list">
            <li>Documentation</li>
            <li>Best Practices</li>
            <li>Support Center</li>
            <li>Training</li>
          </ul>
        </Column>
      </Grid>
    </div>
  </footer>
);

export default Footer;
