// Signup.jsx
import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "../styles/Signup.css";
import { ArrowLeft } from "@carbon/icons-react";
import loginIllustration from "../assets/login-illustration.png";

const Signup = () => {
  const [step, setStep] = useState(1);
  const [accepted, setAccepted] = useState(false);
  const navigate = useNavigate();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  const nextStep = () => setStep(step + 1);
  const prevStep = () => setStep(step - 1);

  const handleCreateAccount = async () => {
    if (!accepted) return;

    try {
      const res = await fetch("http://localhost:8000/auth/signup", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ email, password })
      });

      const data = await res.json();

      if (res.ok) {
        alert("Signup successful! You can now login.");
        navigate("/login");
      } else {
        alert(data.detail || "Signup failed");
      }
    } catch (err) {
      console.error("Signup error:", err);
      alert("Something went wrong!");
    }
  };

  return (
    <div className="login-container">
      <div className="login-left">
        {step > 1 && (
          <button 
            className="back-arrow" 
            onClick={prevStep}
            style={{
              background: "none",
              border: "none",
              marginBottom: "1rem",
              cursor: "pointer",
              color: "white"
            }}
          >
            <ArrowLeft size={24} />
          </button>
        )}

        <h1>{step === 1 ? "Create an IBM Cloud account" : step === 2 ? "Complete account" : "Account Notice"}</h1>
        <p>Already have an IBM Cloud account? <Link to="/login">Log in</Link></p>

        <div className="signup-form">
          {step === 1 && (
            <>
              <label>Email</label>
              <input
                type="email"
                placeholder="Enter your email address"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
              />              
              <label>Password</label>
              <input
                type="password"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              /> 
              <div className="or-separator">OR</div>
              <button className="google-btn">Sign up with Google</button>
              <button 
                onClick={() => {
                  if (!email || !password) {
                    alert("Please fill in email & password");
                  } else {
                    nextStep();
                  }
                }} 
                className="continue-btn"
              >
                Next
              </button>
            </>
          )}
          {step === 2 && (
            <>
              <p><b>Account Information</b>: {email}</p>
              <label>Country or region</label>
              <select>
                <option>India</option>
                <option>United States</option>
              </select>
              <button onClick={nextStep} className="continue-btn">Next</button>
            </>
          )}
          {step === 3 && (
            <>
              <p><b>Account Notice</b></p>
              <label style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem' }}>
                <input 
                  type="checkbox" 
                  checked={accepted} 
                  onChange={(e) => setAccepted(e.target.checked)} 
                />
                I accept the product <a href="#">Terms and Conditions</a>
              </label>
              <p style={{ fontSize: '0.9rem', color: '#a8a8a8' }}>
                By submitting this form, you accept the product <a href="#">Terms and Conditions</a> and acknowledge the IBM privacy statement.
              </p>
              <button 
                className="continue-btn"
                disabled={!accepted}
                onClick={handleCreateAccount}
                style={{
                  opacity: accepted ? 1 : 0.5,
                  cursor: accepted ? 'pointer' : 'not-allowed',
                  marginTop: '1rem'
                }}
              >
                Create account
              </button>
            </>
          )}
        </div>
      </div>

      <div className="login-right">
        <img 
          src={loginIllustration} 
          alt="Signup Illustration"
          style={{ width: "100%", height: "100%", objectFit: "cover" }} 
        />
      </div>
    </div>
  );
};

export default Signup;
