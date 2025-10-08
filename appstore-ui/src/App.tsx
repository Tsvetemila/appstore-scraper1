import React, { useEffect, useState } from "react";

type CompareRow = {
  app_id: string;
  app_name: string;
  developer?: string;
  category?: string;
  subcategory?: string;
  rank?: number;
};

const API = "https://appstore-api.onrender.com";

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly">("compare");
  const [rows, setRows] = useState<CompareRow[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [country, setCountry] = useState("US");
  const [categories, setCategories] = useState<string[]>([]);
  const [category, setCategory] = useState("all");
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [subcategory, setSubcategory] = useState("all");
  const [lastUpdate, setLastUpdate] = useState<string>("—");
  const [sortKey, setSortKey] = useState<"rank" | "app_name">("rank");
  const [sortAsc, setSortAsc] = useState(true);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadMeta();
  }, []);

  useEffect(() => {
    loadCompare();
    loadLatest();
  }, [country, category, subcategory]);

  async function loadMeta() {
    const res = await fetch(`${API}/meta`);
    const data = await res.json();
    setCountries(data.countries);
    setCategories(data.categories);
    setSubcategories(data.subcategories);
  }

  async function loadLatest() {
    const res = await fetch(`${API}/meta/latest-info?country=${country}`);
    const d = await res.json();
    setLastUpdate(d.latest_snapshot || "N/A");
  }

  async function loadCompare() {
    setLoading(true);
    const qs = new URLSearchParams({ country, category, subcategory });
    const res = await fetch(`${API}/compare?${qs.toString()}`);
    const data = await res.json();
    setRows(data.results || []);
    setLoading(false);
  }

  function exportCompareCSV() {
    const qs = new URLSearchParams({ country, category, subcategory });
    window.open(`${API}/compare/export?${qs.toString()}`, "_blank");
  }

  async function refreshDatabase() {
    alert("Refreshing DB from Google Drive...");
    await fetch(`${API}/refresh-db`);
    await loadCompare();
    await loadLatest();
    alert("✅ Database refreshed!");
  }

  const sortedRows = [...rows].sort((a, b) => {
    if (sortKey === "rank") {
      return sortAsc ? (a.rank ?? 0) - (b.rank ?? 0) : (b.rank ?? 0) - (a.rank ?? 0);
    }
    return sortAsc
      ? (a.app_name || "").localeCompare(b.app_name || "")
      : (b.app_name || "").localeCompare(a.app_name || "");
  });

  return (
    <div style={{ fontFamily: "Inter, sans-serif", maxWidth: 1400, margin: "0 auto", padding: 20 }}>
      <h1>📊 App Store Dashboard</h1>

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          {countries.map((c) => (
            <option key={c}>{c}</option>
          ))}
        </select>
        <select value={category} onChange={(e) => setCategory(e.target.value)}>
          <option value="all">All categories</option>
          {categories.map((c) => (
            <option key={c}>{c}</option>
          ))}
        </select>
        <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)}>
          <option value="all">All subcategories</option>
          {subcategories.map((s) => (
            <option key={s}>{s}</option>
          ))}
        </select>
        <button onClick={loadCompare}>Reload</button>
        <button onClick={refreshDatabase}>🔄 Refresh DB</button>
        <button onClick={exportCompareCSV}>⬇️ Export CSV</button>
      </div>

      <p style={{ color: "#666", fontSize: 14 }}>
        Data last updated: <b>{lastUpdate}</b> {loading && <span>(Loading...)</span>}
      </p>

      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead style={{ background: "#f6f6f6" }}>
            <tr>
              <th
                onClick={() => {
                  setSortKey("app_name");
                  setSortAsc(!sortAsc);
                }}
                style={{ cursor: "pointer", textAlign: "left", padding: 8 }}
              >
                App
              </th>
              <th>Developer</th>
              <th
                onClick={() => {
                  setSortKey("rank");
                  setSortAsc(!sortAsc);
                }}
                style={{ cursor: "pointer" }}
              >
                Rank {sortKey === "rank" ? (sortAsc ? "↑" : "↓") : ""}
              </th>
              <th>Category</th>
              <th>Subcategory</th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((r, i) => (
              <tr key={r.app_id + i} style={{ borderBottom: "1px solid #eee" }}>
                <td style={{ padding: 8 }}>{r.app_name}</td>
                <td style={{ padding: 8 }}>{r.developer}</td>
                <td style={{ padding: 8, textAlign: "center" }}>{r.rank}</td>
                <td style={{ padding: 8 }}>{r.category}</td>
                <td style={{ padding: 8 }}>{r.subcategory}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
