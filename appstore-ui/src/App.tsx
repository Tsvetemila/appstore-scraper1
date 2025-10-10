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
  developer?: string | null;
};

type WeeklyInsightRow = {
  status: "NEW" | "RE-ENTRY";
  rank: number | null;
  app_id: string;
  app_name: string;
  first_seen_date: string;
  country: string;
  category: string | null;
  subcategory: string | null;
};

const API = "https://appstore-api.onrender.com";

function formatWeekRange(start?: string | null, end?: string | null) {
  if (!start || !end) return "";
  const s = new Date(start);
  const e = new Date(end);
  const fmt = (d: Date) => d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  const year = e.getFullYear();
  return `${fmt(s)}–${fmt(e)}, ${year}`;
}

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly" | "history">("compare");

  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<string | null>(null);

  const [weeklyRows, setWeeklyRows] = useState<WeeklyInsightRow[]>([]);
  const [weeklyStart, setWeeklyStart] = useState<string | null>(null);
  const [weeklyEnd, setWeeklyEnd] = useState<string | null>(null);
  const [weeklyCounts, setWeeklyCounts] = useState<{ NEW: number; RE_ENTRY: number } | null>(null);
  const [weeklyStatus, setWeeklyStatus] = useState<"all" | "NEW" | "RE-ENTRY">("all");
  const [weeklyCountry, setWeeklyCountry] = useState<string>("US");
  const [weeklyCategory, setWeeklyCategory] = useState<string>("all");
  const [weeklySubcategory, setWeeklySubcategory] = useState<string>("all");

  const [historyRows, setHistoryRows] = useState<any[]>([]);

  async function loadMeta(selectedCategory?: string) {
    const res = await fetch(`${API}/meta${selectedCategory ? `?category=${encodeURIComponent(selectedCategory)}` : ""}`);
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
  }
  useEffect(() => { loadMeta().catch(() => {}); }, []);

  async function loadWeekly() {
    const qs = new URLSearchParams();
    qs.set("country", weeklyCountry);
    if (weeklyCategory !== "all") qs.set("category", weeklyCategory);
    if (weeklySubcategory !== "all") qs.set("subcategory", weeklySubcategory);
    if (weeklyStatus !== "all") qs.set("status", weeklyStatus);
    const r = await fetch(`${API}/weekly/insights?${qs.toString()}`);
    const j = await r.json();
    setWeeklyRows(j.rows ?? []);
    setWeeklyStart(j.week_start ?? null);
    setWeeklyEnd(j.week_end ?? null);
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
    if (j.counts) setWeeklyCounts({ NEW: j.counts.NEW ?? 0, RE_ENTRY: j.counts["RE-ENTRY"] ?? 0 });
  }

  async function loadHistory() {
    const r = await fetch(`${API}/history`);
    const j = await r.json();
    setHistoryRows(j.results ?? []);
  }

  useEffect(() => { if (tab === "weekly") loadWeekly(); else if (tab === "history") loadHistory(); }, [tab]);

  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    qs.set("country", weeklyCountry);
    if (weeklyCategory !== "all") qs.set("category", weeklyCategory);
    if (weeklySubcategory !== "all") qs.set("subcategory", weeklySubcategory);
    if (weeklyStatus !== "all") qs.set("status", weeklyStatus);
    qs.set("format", "csv");
    window.open(`${API}/weekly/insights?${qs.toString()}`, "_blank");
  }

  return (
    <div style={{ fontFamily: "Inter, system-ui, Arial", padding: 16, maxWidth: 1400, margin: "0 auto" }}>
      <h1>📊 App Store Dashboard</h1>

      <div style={{ display: "flex", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <button onClick={() => setTab("compare")}>Compare View</button>
        <button onClick={() => setTab("weekly")}>Weekly Insights</button>
        <button onClick={() => setTab("history")}>History View</button>
      </div>

      <div style={{ marginBottom: 8, color: "#555" }}>Data last updated: <b>{latestSnapshot ?? "N/A"}</b></div>

      {tab === "weekly" && (
        <div>
          <h3>WEEKLY INSIGHTS — (Week of {formatWeekRange(weeklyStart, weeklyEnd)})</h3>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 10 }}>
            <select value={weeklyCountry} onChange={(e) => setWeeklyCountry(e.target.value)}>
              {countries.map((c) => (<option key={c} value={c}>{c}</option>))}
            </select>
            <select value={weeklyCategory} onChange={(e) => setWeeklyCategory(e.target.value)}>
              <option value="all">All categories</option>
              {categories.map((c) => (<option key={c} value={c}>{c}</option>))}
            </select>
            <select value={weeklySubcategory} onChange={(e) => setWeeklySubcategory(e.target.value)}>
              <option value="all">All subcategories</option>
              {subcategories.map((s) => (<option key={s} value={s}>{s}</option>))}
            </select>
            <select value={weeklyStatus} onChange={(e) => setWeeklyStatus(e.target.value as any)}>
              <option value="all">All statuses</option>
              <option value="NEW">NEW</option>
              <option value="RE-ENTRY">RE-ENTRY</option>
            </select>
            <button onClick={() => { setWeeklyCategory("all"); setWeeklySubcategory("all"); setWeeklyStatus("all"); }}>Reset</button>
            <button onClick={loadWeekly}>Reload</button>
            <button onClick={exportWeeklyCSV}>Export CSV</button>
          </div>

          <div style={{ marginBottom: 8, color: "#555" }}>Showing: <b>{weeklyStatus === "all" ? "All" : weeklyStatus}</b>
            {weeklyCounts && (<span style={{ marginLeft: 12 }}>NEW: <b>{weeklyCounts.NEW}</b> • RE-ENTRY: <b>{weeklyCounts.RE_ENTRY}</b></span>)}
          </div>

          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
              <thead style={{ background: "#f7f7f7" }}>
                <tr>
                  <th>Rank</th>
                  <th>App</th>
                  <th>App ID</th>
                  <th>Country</th>
                  <th>Category</th>
                  <th>Subcategory</th>
                  <th>Status</th>
                  <th>First seen</th>
                </tr>
              </thead>
              <tbody>
                {weeklyRows.length === 0 && (<tr><td colSpan={8} style={{ textAlign: "center", color: "#777" }}>No data.</td></tr>)}
                {weeklyRows.map((r, i) => (
                  <tr key={r.app_id + i}>
                    <td>{r.rank ?? "—"}</td>
                    <td>{r.app_name}</td>
                    <td>{r.app_id}</td>
                    <td>{r.country}</td>
                    <td>{r.category ?? "—"}</td>
                    <td>{r.subcategory ?? "—"}</td>
                    <td style={{ color: r.status === "NEW" ? "#007AFF" : "green" }}>{r.status}</td>
                    <td>{r.first_seen_date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === "history" && (
        <div style={{ marginTop: 20 }}>
          <h3>📆 History View (Re-Entry Tracker)</h3>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
              <thead style={{ background: "#f7f7f7" }}>
                <tr>
                  <th>Date</th>
                  <th>Country</th>
                  <th>App</th>
                  <th>App ID</th>
                  <th>Status</th>
                  <th>Rank</th>
                  <th>Replaced ↔ By</th>
                  <th>Replaced Now</th>
                  <th>Prev Rank</th>
                  <th>Curr Rank</th>
                </tr>
              </thead>
              <tbody>
                {historyRows.length === 0 && (<tr><td colSpan={10} style={{ textAlign: "center", color: "#777" }}>No data.</td></tr>)}
                {historyRows.map((r, i) => (
                  <tr key={r.app_id + i}>
                    <td>{r.date}</td>
                    <td>{r.country ?? "—"}</td>
                    <td>{r.app_name}</td>
                    <td>{r.app_id}</td>
                    <td>{r.status}</td>
                    <td>{r.rank}</td>
                    <td>{r.replaced_app_name ?? "—"}</td>
                    <td>{r.replaced_current_rank ?? "—"}</td>
                    <td>{r.previous_rank ?? "—"}</td>
                    <td>{r.current_rank ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
