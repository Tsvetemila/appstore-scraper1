import React, { useEffect, useMemo, useState } from "react";

/* Combined full App.tsx with Compare, Weekly Insights (new logic) and History */

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

  const [country, setCountry] = useState<string>("US");
  const [category, setCategory] = useState<string>("all");
  const [subcategory, setSubcategory] = useState<string>("all");

  const [rows, setRows] = useState<CompareRow[]>([]);
  const [weeklyRows, setWeeklyRows] = useState<WeeklyInsightRow[]>([]);
  const [weeklyStart, setWeeklyStart] = useState<string | null>(null);
  const [weeklyEnd, setWeeklyEnd] = useState<string | null>(null);
  const [weeklyCounts, setWeeklyCounts] = useState<{ NEW: number; RE_ENTRY: number } | null>(null);
  const [weeklyStatus, setWeeklyStatus] = useState<"all" | "NEW" | "RE-ENTRY">("all");

  const [historyRows, setHistoryRows] = useState<any[]>([]);
  const [historyDates, setHistoryDates] = useState<string[]>([]);
  const [historyDate, setHistoryDate] = useState<string>("");
  const [historyStatus, setHistoryStatus] = useState<string>("all");

  async function loadMeta(selectedCategory?: string) {
    const res = await fetch(`${API}/meta${selectedCategory ? `?category=${encodeURIComponent(selectedCategory)}` : ""}`);
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
  }
  useEffect(() => { loadMeta().catch(() => {}); }, []);
  useEffect(() => { loadMeta(category).catch(() => {}); setSubcategory("all"); }, [category]);

  async function loadCompare() {
    const qs = new URLSearchParams();
    qs.set("limit", "50");
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/compare?${qs.toString()}`);
    const j = await r.json();
    setRows(j.results ?? []);
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
  }

  async function loadWeekly() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
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
    const qs = new URLSearchParams();
    if (historyCountry) qs.set("country", historyCountry);
    if (historyStatus !== "all") qs.set("status", historyStatus);
    if (historyDate) qs.set("date", historyDate);
    const r = await fetch(`${API}/history?${qs.toString()}`);
    const j = await r.json();
    setHistoryRows(j.results ?? []);
    setHistoryDates(j.available_dates ?? []);
  }

  const [historyCountry, setHistoryCountry] = useState<string>("US");

  useEffect(() => { if (tab === "compare") loadCompare(); else if (tab === "weekly") loadWeekly(); else loadHistory(); }, [tab]);
  useEffect(() => { if (tab === "compare") loadCompare(); }, [country, category, subcategory, tab]);
  useEffect(() => { if (tab === "weekly") loadWeekly(); }, [country, category, subcategory, weeklyStatus, tab]);

  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
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

      {tab === "weekly" && (
        <div>
          <h3>WEEKLY INSIGHTS — (Week of {formatWeekRange(weeklyStart, weeklyEnd)})</h3>
          <div style={{ marginBottom: 8, color: "#555" }}>Showing: <b>{weeklyStatus}</b>{weeklyCounts && (<span style={{ marginLeft: 12 }}>NEW: <b>{weeklyCounts.NEW}</b> • RE-ENTRY: <b>{weeklyCounts.RE_ENTRY}</b></span>)}</div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
              <thead style={{ background: "#f7f7f7" }}>
                <tr>
                  <th>Rank</th>
                  <th>App</th>
                  <th>App ID</th>
                  <th>Status</th>
                  <th>First seen</th>
                </tr>
              </thead>
              <tbody>
                {weeklyRows.map((r, i) => (
                  <tr key={r.app_id + i}>
                    <td>{r.rank ?? "—"}</td>
                    <td>{r.app_name}</td>
                    <td>{r.app_id}</td>
                    <td style={{ color: r.status === "NEW" ? "#007AFF" : "green" }}>{r.status}</td>
                    <td>{r.first_seen_date}</td>
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
