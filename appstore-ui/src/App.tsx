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
  const [sortAsc, setSortAsc] = useState<boolean>(true);

  const [weeklyRows, setWeeklyRows] = useState<WeeklyInsightRow[]>([]);
  const [weeklyStart, setWeeklyStart] = useState<string | null>(null);
  const [weeklyEnd, setWeeklyEnd] = useState<string | null>(null);
  const [weeklyCounts, setWeeklyCounts] = useState<{ NEW: number; RE_ENTRY: number } | null>(null);
  const [weeklyStatus, setWeeklyStatus] = useState<"all" | "NEW" | "RE-ENTRY">("all");

  const [historyRows, setHistoryRows] = useState<any[]>([]);
  const [historyDates, setHistoryDates] = useState<string[]>([]);
  const [historyDate, setHistoryDate] = useState<string>("");
  const [historyStatus, setHistoryStatus] = useState<string>("all");
  const [historyCountry, setHistoryCountry] = useState<string>("US");
  const [historyCategory, setHistoryCategory] = useState<string>("all");
  const [historySubcategory, setHistorySubcategory] = useState<string>("all");
  const [historySubcatOptions, setHistorySubcatOptions] = useState<string[]>([]);

  async function loadMeta(selectedCategory?: string) {
    const res = await fetch(`${API}/meta${selectedCategory ? `?category=${encodeURIComponent(selectedCategory)}` : ""}`);
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
    if ((m.countries ?? []).length > 0 && !countries.length) setCountry(m.countries[0]);
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

  return (
    <div>
      <h1>WEEKLY INSIGHTS — (Week of {formatWeekRange(weeklyStart, weeklyEnd)})</h1>
      <table>
        <thead>
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
              <td>{r.status}</td>
              <td>{r.first_seen_date}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
