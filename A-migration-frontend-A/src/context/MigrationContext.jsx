import React, { createContext, useContext, useState } from "react";

const MigrationContext = createContext();

export const MigrationProvider = ({ children }) => {
  const [modalOpen, setModalOpen] = useState(false);
  const [currentMigration, setCurrentMigration] = useState(null);

  return (
    <MigrationContext.Provider
      value={{
        modalOpen,
        setModalOpen,
        currentMigration,
        setCurrentMigration
      }}
    >
      {children}
    </MigrationContext.Provider>
  );
};

export const useMigrationContext = () => useContext(MigrationContext);
