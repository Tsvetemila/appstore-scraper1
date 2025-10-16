import React, { useEffect, useMemo, useState } from "react";

const API = "https://appstore-api.onrender.com";

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly">("compare");
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [country, setCountry] = useState("US");
  const [category, setCategory] = useState("all");
  const [subcategory, setSubcategory] = useState("all");
  const [rows, setRows] = useState<any[]>([]);
  const [weeklyRows, setWeeklyRows] = useState<any[]>([]);
  const [page, setPage] = useState(1);
  const perPage = 50;

  async function loadMeta() {
    const res = await fetch(`${API}/meta`);
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
  }

  useEffect(() => {
    loadMeta();
  }, []);

  async function loadCompare() {
    const qs = new URLSearchParams({ limit: "200", country });
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/compare?${qs.toString()}`);
    const j = await r.json();
    setRows(j.results ?? []);
  }

  async function loadWeekly() {
    const qs = new URLSearchParams({ country });
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/weekly/insights?${qs.toString()}`);
    const j = await r.json();
    setWeeklyRows(j.rows ?? []);
  }

  useEffect(() => {
    if (tab === "compare") loadCompare();
    else loadWeekly();
    setPage(1);
  }, [tab, country, category, subcategory]);

  const pagedCompare = useMemo(() => {
    const start = (page - 1) * perPage;
    return rows.slice(start, start + perPage);
  }, [rows, page]);

  const pagedWeekly = useMemo(() => {
    const start = (page - 1) * perPage;
    return weeklyRows.slice(start, start + perPage);
  }, [weeklyRows, page]);

  function changePage(dir: number, total: number) {
    const pages = Math.ceil(total / perPage);
    setPage((p) => Math.min(Math.max(1, p + dir), pages));
  }

  return (
    <div style={{ fontFamily: "Inter, system-ui", padding: 16, maxWidth: 1400, margin: "0 auto" }}>
      <h1>📊 App Store Dashboard</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
        <button onClick={() => setTab("compare")} style={{ background: tab === "compare" ? "#007AFF" : "#ddd" }}>Compare View</button>
        <button onClick={() => setTab("weekly")} style={{ background: tab === "weekly" ? "#007AFF" : "#ddd" }}>Weekly Report</button>
      </div>

      {/* Filters */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 12 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          {countries.map((c) => (<option key={c}>{c}</option>))}
        </select>
        <select value={category} onChange={(e) => setCategory(e.target.value)}>
          <option value="all">All categories</option>
          {categories.map((c) => (<option key={c}>{c}</option>))}
        </select>
        <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)}>
          <option value="all">All subcategories</option>
          {subcategories.map((s) => (<option key={s}>{s}</option>))}
        </select>
      </div>

      {/* Compare View */}
      {tab === "compare" && (
        <>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead><tr>
              <th>App</th><th>App ID</th><th>Country</th><th>Category</th><th>Subcategory</th>
              <th>Current</th><th>Prev</th><th>Δ</th><th>Status</th>
            </tr></thead>
            <tbody>
              {pagedCompare.map((r, i) => (
                <tr key={i}>
                  <td>{r.app_name}</td><td>{r.app_id}</td><td>{r.country}</td>
                  <td>{r.category}</td><td>{r.subcategory}</td>
                  <td>{r.current_rank ?? "—"}</td><td>{r.previous_rank ?? "—"}</td>
                  <td>{r.delta ?? "—"}</td><td>{r.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ marginTop: 10 }}>
            <button onClick={() => changePage(-1, rows.length)}>Prev</button>
            <span style={{ margin: "0 8px" }}>Page {page}</span>
            <button onClick={() => changePage(1, rows.length)}>Next</button>
          </div>
        </>
      )}

      {/* Weekly Report */}
      {tab === "weekly" && (
        <>
          <h2 style={{ marginTop: 20 }}>WEEKLY INSIGHTS</h2>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th>Status</th><th>Rank</th><th>App</th><th>App ID</th>
                <th>Developer</th><th>Bundle ID</th><th>Category</th><th>Subcategory</th>
              </tr>
            </thead>
            <tbody>
              {pagedWeekly.map((r, i) => (
                <tr key={i}>
                  <td>{r.status}</td>
                  <td>{r.rank ?? "—"}</td>
                  <td>{r.app_name}</td>
                  <td>{r.app_id}</td>
                  <td>{r.developer_name}</td>
                  <td>{r.bundle_id}</td>
                  <td>{r.category}</td>
                  <td>{r.subcategory}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ marginTop: 10 }}>
            <button onClick={() => changePage(-1, weeklyRows.length)}>Prev</button>
            <span style={{ margin: "0 8px" }}>Page {page}</span>
            <button onClick={() => changePage(1, weeklyRows.length)}>Next</button>
          </div>
        </>
      )}
    </div>
  );
}
