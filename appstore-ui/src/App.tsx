import React, { useEffect, useState } from "react";

type CompareRow = {
  app_id: string;
  app_name: string;
  current_rank: number | null;
  previous_rank: number | null;
  delta: number | null;
  status: string;
  country: string | null;
  category: string | null;
  subcategory: string | null;
};

type WeeklyRow = {
  country: string;
  category: string;
  subcategory: string;
  rank: number;
  app_id: string;
  app_name: string;
  developer_name: string;
};

// ✅ Промяната е само тук:
const API = "https://appstore-api.onrender.com";

function StatusIcon({ row }: { row: CompareRow }) {
  switch (row.status) {
    case "NEW": return <span style={{ color: "blue" }}>🆕</span>;
    case "MOVER UP": return <span style={{ color: "green" }}>▲ {row.delta}</span>;
    case "MOVER DOWN": return <span style={{ color: "red" }}>▼ {Math.abs(row.delta ?? 0)}</span>;
    case "DROPOUT": return <span style={{ color: "gray" }}>❌</span>;
    case "RE-ENTRY": return <span style={{ color: "purple" }}>🔄</span>;
    default: return <span style={{ color: "gray" }}>→</span>;
  }
}

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly">("compare");
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);

  const [country, setCountry] = useState("all");
  const [category, setCategory] = useState("all");
  const [subcategory, setSubcategory] = useState("all");

  const [rows, setRows] = useState<CompareRow[]>([]);
  const [weekly, setWeekly] = useState<{ new: WeeklyRow[]; dropped: WeeklyRow[] }>({ new: [], dropped: [] });

  // Load META
  useEffect(() => {
    (async () => {
      const res = await fetch(`${API}/meta`);
      const m = await res.json();
      setCountries(m.countries ?? []);
      setCategories(m.categories ?? []);
      setSubcategories(m.subcategories ?? []);
    })();
  }, []);

  // Load compare7 data
  async function loadCompare() {
    const qs = new URLSearchParams();
    qs.set("limit", "50");
    if (country !== "all") qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);

    const r = await fetch(`${API}/compare7?${qs.toString()}`);
    const j = await r.json();
    setRows(j.results ?? []);
  }

  // Load weekly NEW/DROPOUT
  async function loadWeekly() {
    const qs = new URLSearchParams();
    if (country !== "all") qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/reports/weekly?${qs.toString()}`);
    const j = await r.json();
    setWeekly({ new: j.new ?? [], dropped: j.dropped ?? [] });
  }

  useEffect(() => {
    if (tab === "compare") loadCompare();
    else loadWeekly();
  }, [country, category, subcategory, tab]);

  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    if (country !== "all") qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    qs.set("format", "csv");
    window.open(`${API}/reports/weekly?${qs.toString()}`, "_blank");
  }

  return (
    <div style={{ fontFamily: "Inter, Arial", padding: 20, maxWidth: 1400, margin: "0 auto" }}>
      <h1>📊 App Store Dashboard</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
        <button
          style={{ background: tab === "compare" ? "#007AFF" : "#ccc", color: "#fff", padding: "6px 14px", borderRadius: 6 }}
          onClick={() => setTab("compare")}
        >
          Compare View
        </button>
        <button
          style={{ background: tab === "weekly" ? "#007AFF" : "#ccc", color: "#fff", padding: "6px 14px", borderRadius: 6 }}
          onClick={() => setTab("weekly")}
        >
          Weekly Report
        </button>
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          <option value="all">All countries</option>
          {countries.map((c) => <option key={c}>{c}</option>)}
        </select>

        <select value={category} onChange={(e) => { setCategory(e.target.value); setSubcategory("all"); }}>
          <option value="all">All categories</option>
          {categories.map((c) => <option key={c}>{c}</option>)}
        </select>

        <select
          value={subcategory}
          onChange={(e) => setSubcategory(e.target.value)}
          disabled={category !== "Games"}
        >
          <option value="all">All subcategories</option>
          {subcategories.map((s) => <option key={s}>{s}</option>)}
        </select>

        <button onClick={() => { setCountry("all"); setCategory("all"); setSubcategory("all"); }}>
          Reset
        </button>
        {tab === "weekly" && (
          <button onClick={exportWeeklyCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>
            Export CSV
          </button>
        )}
      </div>

      {/* Compare View */}
      {tab === "compare" && (
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th>App</th><th>Country</th><th>Category</th><th>Subcategory</th>
              <th>Current</th><th>Previous</th><th>Δ</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={r.app_id + i}>
                <td>{r.app_name}</td>
                <td>{r.country}</td>
                <td>{r.category}</td>
                <td>{r.subcategory ?? "—"}</td>
                <td>{r.current_rank ?? "—"}</td>
                <td>{r.previous_rank ?? "—"}</td>
                <td>{r.delta ?? "—"}</td>
                <td><StatusIcon row={r} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Weekly Report View */}
      {tab === "weekly" && (
        <div style={{ display: "flex", gap: 40 }}>
          <div style={{ flex: 1 }}>
            <h3>🆕 New Entries ({weekly.new.length})</h3>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr><th>Rank</th><th>App</th><th>Dev</th></tr></thead>
              <tbody>
                {weekly.new.map((a, i) => (
                  <tr key={a.app_id + i}><td>{a.rank}</td><td>{a.app_name}</td><td>{a.developer_name}</td></tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={{ flex: 1 }}>
            <h3>❌ Dropped ({weekly.dropped.length})</h3>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr><th>Rank</th><th>App</th><th>Dev</th></tr></thead>
              <tbody>
                {weekly.dropped.map((a, i) => (
                  <tr key={a.app_id + i}><td>{a.rank}</td><td>{a.app_name}</td><td>{a.developer_name}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
