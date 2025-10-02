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

const API = "http://127.0.0.1:8000";

function StatusIcon({ row }: { row: CompareRow }) {
  switch (row.status) {
    case "NEW": return <span style={{ color: "blue" }}>🆕</span>;
    case "MOVER UP": return <span style={{ color: "green" }}>▲ {row.delta}</span>;
    case "MOVER DOWN": return <span style={{ color: "red" }}>▼ {Math.abs(row.delta ?? 0)}</span>;
    case "DROPOUT": return <span style={{ color: "gray" }}>❌</span>;
    default: return <span style={{ color: "gray" }}>→</span>;
  }
}

export default function App() {
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);

  const [country, setCountry] = useState("all");
  const [category, setCategory] = useState("all");
  const [subcategory, setSubcategory] = useState("all");

  const [rows, setRows] = useState<CompareRow[]>([]);

  useEffect(() => {
    (async () => {
      const res = await fetch(`${API}/meta`);
      const m = await res.json();
      setCountries(m.countries ?? []);
      setCategories(m.categories ?? []);
      setSubcategories(m.subcategories ?? []);
    })();
  }, []);

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

  useEffect(() => { loadCompare(); }, [country, category, subcategory]);

  return (
    <div style={{ fontFamily: "Inter, Arial", padding: 20, maxWidth: 1400, margin: "0 auto" }}>
      <h1>📊 App Store Dashboard</h1>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          <option value="all">All countries</option>
          {countries.map((c) => <option key={c}>{c}</option>)}
        </select>

        <select value={category} onChange={(e) => { setCategory(e.target.value); setSubcategory("all"); }}>
          <option value="all">All categories</option>
          {categories.map((c) => <option key={c}>{c}</option>)}
        </select>

        <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)} disabled={category !== "Games"}>
          <option value="all">All subcategories</option>
          {subcategories.map((s) => <option key={s}>{s}</option>)}
        </select>

        <button onClick={loadCompare}>Apply</button>
        <button onClick={() => { setCountry("all"); setCategory("all"); setSubcategory("all"); loadCompare(); }}>Reset</button>
      </div>

      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th>App</th><th>Country</th><th>Category</th><th>Subcategory</th><th>Current</th><th>Previous</th><th>Δ</th><th>Status</th>
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
    </div>
  );
}
