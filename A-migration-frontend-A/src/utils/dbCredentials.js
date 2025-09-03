// src/utils/dbCredentials.js
export const saveCredentials = (key, creds) => {
  sessionStorage.setItem(key, JSON.stringify(creds));
};

export const getCredentials = (key) => {
  const data = sessionStorage.getItem(key);
  return data ? JSON.parse(data) : null;
};
