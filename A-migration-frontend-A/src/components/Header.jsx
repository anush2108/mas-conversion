import React, { useContext, useState, useRef } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import "../styles/Header.css";
import { Button } from "@carbon/react";
import "@carbon/styles/css/styles.css";
import { UserAvatarFilledAlt } from "@carbon/icons-react";
import Sidebar from "./Sidebar";
import { AuthContext } from "../context/AuthContext";

const Header = () => {
  const { isLoggedIn, setIsLoggedIn } = useContext(AuthContext);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const dropdownRef = useRef(null);

  // No local login state or login check here anymore

  React.useEffect(() => {
    // Close dropdown when clicking outside
    const handleClickOutside = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleLogout = async () => {
    try {
      await fetch("https://backend-mas-conversion.apps.6890779dfbf8f4f78fdef06a.am1.techzone.ibm.com/auth/logout", { method: "POST", credentials: "include" });
    } catch {
      // ignore error
    }
    setIsLoggedIn(false); // update shared auth state immediately
    setDropdownOpen(false);
    navigate("/");
  };

  return (
    <>
      <Sidebar />

      <header className="landing-header">
        <div className="container">
          <div className="header-content">
            <div className="left-logo">
              <Link to="/">
                <img src="/ibm.png" alt="IBM Logo" className="ibm-logo" />
              </Link>
            </div>

            <nav className={`nav-links ${isLoggedIn ? "shift-right" : ""}`}>
              <Link to="/products">Products</Link>
              <Link to="/#solutions">Solutions</Link>
              <Link to="/#support">Support</Link>
              <Link to="/#docs">Documentation</Link>
            </nav>

            <div className="auth-buttons">
              {isLoggedIn ? (
                <div className="profile-dropdown" ref={dropdownRef}>
                  <UserAvatarFilledAlt
                    className="profile-icon"
                    size={36}
                    onClick={() => setDropdownOpen(!dropdownOpen)}
                    style={{ cursor: "pointer" }}
                  />
                  {dropdownOpen && (
                    <div className="dropdown-menu">
                      <Link to="/account">Account Settings</Link>
                      <button onClick={handleLogout}>Logout</button>
                    </div>
                  )}
                </div>
              ) : (
                <>
                  <Link to="/login">
                    <Button kind="secondary" size="md" className="login-btn">Login</Button>
                  </Link>

                  <Link to="/signup">
                    <Button kind="primary" size="md" className="signup-btn">Sign Up</Button>
                  </Link>
                </>
              )}
            </div>
          </div>
        </div>
      </header>
    </>
  );
};

export default Header;
