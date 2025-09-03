import React from "react";
import { BrowserRouter as Router, Routes, Route, useLocation } from "react-router-dom";

import LandingPage from "./pages/LandingPage";
import TableMigration from "./pages/TableMigration";
import TableDataView from "./pages/TableDataView";
import ProductsPage from "./pages/ProductsPage";
import DataValidationPage from "./pages/DataValidationPage";
import SequenceMigration from "./pages/SequenceMigration";
import TriggerMigration from "./pages/TriggerMigration";
import IndexMigration from "./pages/IndexMigration";
import LoginPage from "./pages/LoginPage";
import Signup from "./pages/Signup";
import ViewMigration from "./pages/ViewMigration";
import AccountSettings from "./pages/AccountSettings";
import PrivateRoute from "./components/PrivateRoute";
import FullSchemaMigration from "./pages/FullSchemaMigration";
import Dashboard from "./pages/Dashboard";
import MaximoDataFAQ from "./components/MaximoDataFAQ";
import EmbeddedSQLMigrationPage from "./pages/EmbeddedSQLMigrationPage";

import Sidebar from "./components/Sidebar";
import { MigrationProvider } from "./context/MigrationContext";
import GlobalMigrationStatusModal from "./components/GlobalMigrationStatusModal";

// Helper component so we can use hooks like useLocation:
const AppContent = () => {
  const location = useLocation();
  // Add all routes where the sidebar should NOT appear
  const noSidebarRoutes = ["/login", "/signup", "/faqs", "/account"];

  const showSidebar = !noSidebarRoutes.includes(location.pathname);

  return (
    <>
      {showSidebar && <Sidebar />}
      <Routes>
        {/* Public Routes */}
        <Route path="/" element={<LandingPage />} />
        <Route path="/login" element={<LoginPage />} />
        <Route path="/signup" element={<Signup />} />
        <Route path="/faqs" element={<MaximoDataFAQ />} />
        {/* Protected Routes */}
        <Route path="/dashboard" element={<PrivateRoute><Dashboard /></PrivateRoute>} />
        <Route path="/products" element={<PrivateRoute><ProductsPage /></PrivateRoute>} />
        <Route path="/table-migration" element={<PrivateRoute><TableMigration /></PrivateRoute>} />
        <Route path="/index-migration" element={<PrivateRoute><IndexMigration /></PrivateRoute>} />
        <Route path="/table-data" element={<PrivateRoute><TableDataView /></PrivateRoute>} />
        <Route path="/validate" element={<PrivateRoute><DataValidationPage /></PrivateRoute>} />
        <Route path="/sequences" element={<PrivateRoute><SequenceMigration /></PrivateRoute>} />
        <Route path="/trigger-migration" element={<PrivateRoute><TriggerMigration /></PrivateRoute>} />
        <Route path="/views" element={<PrivateRoute><ViewMigration /></PrivateRoute>} />
        <Route path="/account" element={<PrivateRoute><AccountSettings /></PrivateRoute>} />
        <Route path="/full-schema-migration" element={<PrivateRoute><FullSchemaMigration /></PrivateRoute>} />
        <Route path="/embedded-sql-migration" element={<PrivateRoute><EmbeddedSQLMigrationPage /></PrivateRoute>} />
        
      </Routes>
      {/* Global Migration Status Modal */}
      <GlobalMigrationStatusModal />
    </>
  );
};

const App = () => (
  <MigrationProvider>
    <Router>
      <AppContent />
    </Router>
  </MigrationProvider>
);

export default App;
