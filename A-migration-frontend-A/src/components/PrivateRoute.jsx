// src/components/PrivateRoute.jsx
import React, { useContext } from "react";
import { Navigate } from "react-router-dom";
import { AuthContext } from "../context/AuthContext"; // Adjust the import path as needed

const PrivateRoute = ({ children }) => {
  const { isLoggedIn } = useContext(AuthContext);

  // Optionally, you can handle a loading state if isLoggedIn is initially null
  if (isLoggedIn === null) {
    return <div>Loading...</div>; // Or replace with a nicer loading spinner
  }

  return isLoggedIn ? children : <Navigate to="/login" replace />;
};

export default PrivateRoute;
