import React, { useState, useContext } from "react";
import { Link, useNavigate } from "react-router-dom";
import { AuthContext } from "../context/AuthContext";  // Adjust path if needed
import "../styles/LoginPage.css";
import loginImage from "../assets/login-illustration.png";

const Login = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showSuccess, setShowSuccess] = useState(false);
  const navigate = useNavigate();

  const { setIsLoggedIn } = useContext(AuthContext);

  const handleLogin = async () => {
    if (!email || !password) {
      alert("Fill the above details to continue");
      return;
    }

    try {
      const res = await fetch("http://localhost:8000/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ email, password }),
      });

      const data = await res.json();

      if (res.ok) {
        setIsLoggedIn(true);  // <-- Update auth context state here
        setShowSuccess(true);
        setTimeout(() => {
          setShowSuccess(false);
          navigate("/");
        }, 2500);
      } else {
        alert(data.detail || "Invalid login");
      }
    } catch (err) {
      console.error("Login error:", err);
      alert("Login failed");
    }
  };

  return (
    <div className="login-container">
      <div className="login-left">
        <h1>
          Log in to <span className="highlight">IBM <span className="cloud-text">Cloud</span></span>
        </h1>
        <p>
          Don’t have an account? <Link to="/signup">Create one</Link>
        </p>

        <div className="login-form">
          <label className="form-label">Sign in with</label>
          <input
            type="email"
            placeholder="username@example.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="input-field"
          />
          <input
            type="password"
            placeholder="Enter password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="input-field"
          />
          <button onClick={handleLogin} className="continue-btn">
            Continue
          </button>
          <Link to="/account" className="forgot-link">
            Forgot ID?
          </Link>
          <div className="or-separator">or</div>
          <button className="google-btn">Sign in with Google</button>
        </div>

        <div className="footer">
          <p>
            © Copyright IBM Corp. 2014, 2025. All rights reserved. <Link to="#">Privacy</Link>
          </p>
        </div>
      </div>

      <div className="login-right">
        <img src={loginImage} alt="Login Illustration" />
      </div>

      {showSuccess && (
        <div className="success-dialog">
          <div className="success-box">
            <div className="success-icon">✅</div>
            <p className="success-message">Login Successful!</p>
            <p className="success-subtext">Get Started with Database Conversion....</p>
          </div>
        </div>
      )}
    </div>
  );
};

export default Login;
