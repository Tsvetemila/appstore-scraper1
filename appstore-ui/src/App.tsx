import React, { useEffect, useMemo, useState } from "react";

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
  developer?: string | null;
};

type WeeklyRow = {
  rank: number | null;
  app_id: string;
  app_name: string;
  developer_name: string;
};

const API = "https://appstore-api.onrender.com";

function StatusIcon({ row }: { row: CompareRow }) {
  switch (row.status) {
    case "NEW":
      return <span style={{ color: "#1f6feb" }}>🆕</span>;
    case "MOVER UP":
      return <span style={{ color: "green" }}>▲ {row.delta}</span>;
    case "MOVER DOWN":
      return <span style={{ color: "red" }}>▼ {Math.abs(row.delta ?? 0)}</span>;
    case "DROPPED":
      return <span style={{ color: "#999" }}>❌</span>;
    default:
      return <span style={{ color: "#777" }}>→</span>;
  }
}

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly">("compare");

  // meta
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);

  // filters
  const [country, setCountry] = useState<string>("US");
  const [category, setCategory] = useState<string>("all");
  const [subcategory, setSubcategory] = useState<string>("all");

  // data
  const [rows, setRows] = useState<CompareRow[]>([]);
  const [weekly, setWeekly] = useState<{ new: WeeklyRow[]; dropped: WeeklyRow[] }>({ new: [], dropped: [] });

  // ---------- load META once ----------
  useEffect(() => {
    (async () => {
      const res = await fetch(`${API}/meta`);
      const m = await res.json();
      setCountries(m.countries ?? []);
      setCategories(m.categories ?? []);
      setSubcategories(m.subcategories ?? []);
      // default country – first in list if exists
      if ((m.countries ?? []).length > 0) setCountry(m.countries[0]);
    })();
  }, []);

  // ---------- reload subcategories when category changes ----------
  useEffect(() => {
    (async () => {
      const res = await fetch(`${API}/meta?category=${encodeURIComponent(category)}`);
      const m = await res.json();
      setSubcategories(m.subcategories ?? []);
    })();
  }, [category]);

  // ---------- data loaders ----------
  async function loadCompare() {
    const qs = new URLSearchParams();
    qs.set("limit", "50");
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/compare?${qs.toString()}`);
    const j = await r.json();
    setRows(j.results ?? []);
  }

  async function loadWeekly() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/reports/weekly?${qs.toString()}`);
    const j = await r.json();
    setWeekly({ new: j.new ?? [], dropped: j.dropped ?? [] });
  }

  useEffect(() => {
    if (!country) return;
    if (tab === "compare") loadCompare();
    else loadWeekly();
  }, [country, category, subcategory, tab]);

  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    qs.set("format", "csv");
    window.open(`${API}/reports/weekly?${qs.toString()}`, "_blank");
  }

  // ---------- UI ----------
  return (
    <div style={{ fontFamily: "Inter, Arial", padding: 16, maxWidth: 1400, margin: "0 auto" }}>
      <h1>📊 App Store Dashboard</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
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
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 16 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          {countries.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select
          value={category}
          onChange={(e) => {
            setCategory(e.target.value);
            setSubcategory("all");
          }}
        >
          <option value="all">All categories</option>
          {categories.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)}>
          <option value="all">All subcategories</option>
          {subcategories.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>

        <button
          onClick={() => {
            setCategory("all");
            setSubcategory("all");
          }}
        >
          Reset
        </button>

        {tab === "weekly" && (
          <button
            onClick={exportWeeklyCSV}
            style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}
          >
            Export CSV
          </button>
        )}
      </div>

      {/* Compare View */}
      {tab === "compare" && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #ddd" }}>
            <thead style={{ background: "#f5f5f5" }}>
              <tr>
                <th style={{ textAlign: "left", padding: 8 }}>App</th>
                <th style={{ textAlign: "left", padding: 8 }}>Country</th>
                <th style={{ textAlign: "left", padding: 8 }}>Category</th>
                <th style={{ textAlign: "left", padding: 8 }}>Subcategory</th>
                <th style={{ textAlign: "right", padding: 8 }}>Current</th>
                <th style={{ textAlign: "right", padding: 8 }}>Previous (avg 7d)</th>
                <th style={{ textAlign: "right", padding: 8 }}>Δ</th>
                <th style={{ textAlign: "center", padding: 8 }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={r.app_id + i} style={{ borderBottom: "1px solid #eee" }}>
                  <td style={{ padding: 8 }}>{r.app_name}</td>
                  <td style={{ padding: 8 }}>{r.country}</td>
                  <td style={{ padding: 8 }}>{r.category}</td>
                  <td style={{ padding: 8 }}>{r.subcategory ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "right" }}>{r.current_rank ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "right" }}>{r.previous_rank ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "right" }}>{r.delta ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "center" }}>
                    <StatusIcon row={r} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Weekly Report View */}
      {tab === "weekly" && (
        <div style={{ display: "flex", gap: 40, flexWrap: "wrap" }}>
          <div style={{ flex: 1, minWidth: 450 }}>
            <h3>🆕 New Entries ({weekly.new.length})</h3>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "right", padding: 6 }}>Rank</th>
                    <th style={{ textAlign: "left", padding: 6 }}>App</th>
                    <th style={{ textAlign: "left", padding: 6 }}>Developer</th>
                  </tr>
                </thead>
                <tbody>
                  {weekly.new.map((a, i) => (
                    <tr key={a.app_id + i}>
                      <td style={{ textAlign: "right", padding: 6 }}>{a.rank ?? "—"}</td>
                      <td style={{ padding: 6 }}>{a.app_name}</td>
                      <td style={{ padding: 6 }}>{a.developer_name}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div style={{ flex: 1, minWidth: 450 }}>
            <h3>❌ Dropped ({weekly.dropped.length})</h3>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "right", padding: 6 }}>Rank</th>
                    <th style={{ textAlign: "left", padding: 6 }}>App</th>
                    <th style={{ textAlign: "left", padding: 6 }}>Developer</th>
                  </tr>
                </thead>
                <tbody>
                  {weekly.dropped.map((a, i) => (
                    <tr key={a.app_id + i}>
                      <td style={{ textAlign: "right", padding: 6 }}>{a.rank ?? "—"}</td>
                      <td style={{ padding: 6 }}>{a.app_name}</td>
                      <td style={{ padding: 6 }}>{a.developer_name}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
