import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { ArrowLeft } from "@carbon/icons-react";
import "../styles/Signup.css";
import loginIllustration from "../assets/login-illustration.png";

const AccountSettings = () => {
  const [step, setStep] = useState(1);
  const [email, setEmail] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [accepted, setAccepted] = useState(false);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    const validateSession = async () => {
      try {
        const res = await fetch("https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com/auth/account", {
          credentials: "include",
        });
        const data = await res.json();

        if (!res.ok || !data?.email) {
          navigate("/login", { replace: true });
        } else {
          setEmail(data.email);
        }
      } catch {
        navigate("/login", { replace: true });
      } finally {
        setLoading(false);
      }
    };

    validateSession();

    const handleVisibility = () => {
      if (document.visibilityState === "visible") {
        validateSession();
      }
    };

    document.addEventListener("visibilitychange", handleVisibility);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibility);
    };
  }, [navigate]);

  const handlePasswordUpdate = async () => {
    if (!accepted) return;

    if (!newPassword || !confirmPassword) {
      alert("Please fill both password fields");
      return;
    }

    if (newPassword !== confirmPassword) {
      alert("❌ Passwords do not match");
      return;
    }

    if (newPassword.length < 6) {
      alert("❌ Password must be at least 6 characters long");
      return;
    }

    try {
      const res = await fetch("https://mas-migration-backend-1-open-db.apps.itz-47ubpb.infra01-lb.dal14.techzone.ibm.com/auth/account/change-password", {
        method: "POST",
        credentials: "include",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ new_password: newPassword }),
      });

      const data = await res.json();

      if (res.ok) {
        alert("✅ Password updated successfully. Please log in again.");
        navigate("/login", { replace: true });
      } else {
        alert(data.detail || "❌ Failed to update password");
      }
    } catch (err) {
      console.error("Update error:", err);
      alert("❌ An error occurred while updating password");
    }
  };

  if (loading) {
    return (
      <div className="login-container">
        <div className="login-left">
          <p>Loading account info...</p>
        </div>
        <div className="login-right">
          <img src={loginIllustration} alt="loading" />
        </div>
      </div>
    );
  }

  return (
    <div className="login-container">
      <div className="login-left">
        {step > 1 && (
          <button className="back-arrow" onClick={() => setStep(step - 1)}>
            <ArrowLeft size={24} />
          </button>
        )}

        <h1>
          {step === 1
            ? "Account Settings"
            : step === 2
            ? "Update Password"
            : "Confirm Update"}
        </h1>
        <p>Signed in as: <b>{email}</b></p>

        <div className="signup-form">
          {step === 1 && (
            <>
              <p>You can update your password.</p>
              <button onClick={() => setStep(2)} className="continue-btn">
                Change Password
              </button>
            </>
          )}

          {step === 2 && (
            <>
              <label>New Password</label>
              <input
                type="password"
                placeholder="Enter new password"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
              />
              <label>Confirm Password</label>
              <input
                type="password"
                placeholder="Confirm new password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
              />
              <button
                className="continue-btn"
                onClick={() => {
                  if (!newPassword || !confirmPassword) {
                    alert("Please fill in both fields");
                  } else {
                    setStep(3);
                  }
                }}
              >
                Next
              </button>
            </>
          )}

          {step === 3 && (
            <>
              <label style={{ display: "flex", gap: "0.5rem" }}>
                <input
                  type="checkbox"
                  checked={accepted}
                  onChange={(e) => setAccepted(e.target.checked)}
                />
                I confirm I want to update my password
              </label>
              <p style={{ fontSize: "0.9rem", color: "#a8a8a8" }}>
                You will be logged out after password update.
              </p>
              <button
                className="continue-btn"
                disabled={!accepted}
                onClick={handlePasswordUpdate}
                style={{
                  opacity: accepted ? 1 : 0.5,
                  cursor: accepted ? "pointer" : "not-allowed",
                }}
              >
                Update Password
              </button>
            </>
          )}
        </div>
      </div>

      <div className="login-right">
        <img
          src={loginIllustration}
          alt="Account Illustration"
          style={{ width: "100%", height: "100%", objectFit: "cover" }}
        />
      </div>
    </div>
  );
};

export default AccountSettings;
