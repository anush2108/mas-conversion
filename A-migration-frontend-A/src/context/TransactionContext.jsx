// import React, { createContext, useState, useContext } from 'react';

// const TransactionContext = createContext();

// export const TransactionProvider = ({ children }) => {
//   const [selectedSchema, setSelectedSchema] = useState(null);
//   const [transactionId, setTransactionId] = useState(null);

//   return (
//     <TransactionContext.Provider value={{
//       selectedSchema,
//       setSelectedSchema,
//       transactionId,
//       setTransactionId
//     }}>
//       {children}
//     </TransactionContext.Provider>
//   );
// };

// export const useTransaction = () => {
//   const context = useContext(TransactionContext);
//   if (!context) throw new Error('useTransaction must be used within a TransactionProvider');
//   return context;
// };
