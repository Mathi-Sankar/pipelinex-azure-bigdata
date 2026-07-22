// API base URL. In production, set VITE_API_URL to the Render backend URL.
// In dev it falls back to the local FastAPI server.
const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";

export async function fetchJSON(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json();
}
