import React, { useEffect, useMemo, useState } from "react";

/* ----------------------------- Types ----------------------------- */

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
  // ако бекендът върне и тези полета – ще ги визуализираме:
  last_week_rank?: number | null;
  rank_delta?: number | null; // положително = нагоре
};

type WeeklyMoverRow = {
  rank: number | null;
  last_week_rank: number | null;
  rank_delta: number | null; // положително = нагоре
  app_id: string;
  app_name: string;
  country: string;
  category: string | null;
  subcategory: string | null;
};

const API = "https://appstore-api.onrender.com";

/* ----------------------------- Helpers ----------------------------- */

function formatWeekRange(start?: string | null, end?: string | null) {
  if (!start || !end) return "";
  const s = new Date(start);
  const e = new Date(end);
  const fmt = (d: Date) =>
    d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  const year = e.getFullYear();
  return `${fmt(s)}–${fmt(e)}, ${year}`;
}

function chip(color: string, text: string) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 8px",
        borderRadius: 999,
        background: color,
        color: "#fff",
        fontSize: 12,
        lineHeight: "18px",
      }}
    >
      {text}
    </span>
  );
}

function statusChip(s: string) {
  if (s === "NEW") return chip("#007AFF", "NEW");
  if (s === "RE-ENTRY") return chip("#2E7D32", "RE-ENTRY");
  if (s === "DROPPED") return chip("#A61B1B", "DROPPED");
  return chip("#6B7280", s);
}

function sectionCard(children: React.ReactNode) {
  return (
    <div
      style={{
        background: "#fff",
        border: "1px solid #eaeaea",
        borderRadius: 12,
        boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
        padding: 12,
      }}
    >
      {children}
    </div>
  );
}

function tableStyles() {
  return {
    table: {
      width: "100%",
      borderCollapse: "separate" as const,
      borderSpacing: 0,
      background: "#fff",
      border: "1px solid #eee",
      borderRadius: 12,
      overflow: "hidden",
    },
    th: {
      textAlign: "left" as const,
      padding: 10,
      fontWeight: 600,
      fontSize: 13,
      background: "#f7f8fa",
      borderBottom: "1px solid #eee",
      whiteSpace: "nowrap" as const,
    },
    td: {
      padding: 10,
      fontSize: 13,
      borderBottom: "1px solid #f3f3f3",
      verticalAlign: "middle" as const,
    },
  };
}

function rowBgByStatus(s?: "NEW" | "RE-ENTRY") {
  if (s === "NEW") return "#f0f7ff"; // леко синьо
  if (s === "RE-ENTRY") return "#f0fff4"; // леко зелено
  return undefined;
}

function StatusIcon({ row }: { row: CompareRow }) {
  const d = row.delta ?? 0;
  switch (row.status) {
    case "NEW":
      return <span title="New">🆕</span>;
    case "MOVER UP":
      return <span style={{ color: "green" }} title="Up">▲ {d}</span>;
    case "MOVER DOWN":
      return <span style={{ color: "#cc0000" }} title="Down">▼ {Math.abs(d)}</span>;
    case "DROPPED":
      return <span style={{ color: "#999" }} title="Dropped">❌</span>;
    default:
      return <span style={{ color: "#777" }} title="No change">→</span>;
  }
}

/* ----------------------------- Component ----------------------------- */

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly" | "history">("compare");

  // META (глобални списъци)
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<string | null>(null);

  // глобални филтри (Compare)
  const [country, setCountry] = useState<string>("US");
  const [category, setCategory] = useState<string>("all");
  const [subcategory, setSubcategory] = useState<string>("all");

  // Compare данни
  const [rows, setRows] = useState<CompareRow[]>([]);
  const [sortAsc, setSortAsc] = useState<boolean>(true);

  // Weekly – вътрешни табове и филтри (самостоятелни за изгледа)
  const [weeklyInnerTab, setWeeklyInnerTab] = useState<"insights" | "movers">(
    "insights"
  );
  const [weeklyRows, setWeeklyRows] = useState<WeeklyInsightRow[]>([]);
  const [moverRows, setMoverRows] = useState<WeeklyMoverRow[]>([]);
  const [weeklyStart, setWeeklyStart] = useState<string | null>(null);
  const [weeklyEnd, setWeeklyEnd] = useState<string | null>(null);
  const [weeklyCounts, setWeeklyCounts] = useState<{
    NEW: number;
    RE_ENTRY: number;
  } | null>(null);

  // вътрешни филтри
  const [weeklyCountry, setWeeklyCountry] = useState<string>(country);
  const [weeklyCategory, setWeeklyCategory] = useState<string>("all");
  const [weeklySubcategory, setWeeklySubcategory] = useState<string>("all");
  const [weeklyStatus, setWeeklyStatus] = useState<"all" | "NEW" | "RE-ENTRY">(
    "all"
  );
  const [weeklySortByStatusAsc, setWeeklySortByStatusAsc] = useState(true);

  // History – отделни филтри и данни
  const [historyRows, setHistoryRows] = useState<any[]>([]);
  const [historyDates, setHistoryDates] = useState<string[]>([]);
  const [historyDate, setHistoryDate] = useState<string>("");
  const [historyStatus, setHistoryStatus] = useState<string>("all");
  const [historyCountry, setHistoryCountry] = useState<string>("US");
  const [historyCategory, setHistoryCategory] = useState<string>("all");
  const [historySubcategory, setHistorySubcategory] = useState<string>("all");
  const [historySubcatOptions, setHistorySubcatOptions] = useState<string[]>(
    []
  );

  /* -------------------------- Loaders & Meta -------------------------- */

  async function loadMeta(selectedCategory?: string) {
    const res = await fetch(
      `${API}/meta${
        selectedCategory ? `?category=${encodeURIComponent(selectedCategory)}` : ""
      }`
    );
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
    if ((m.countries ?? []).length > 0 && !countries.length) setCountry(m.countries[0]);
  }
  useEffect(() => {
    loadMeta().catch(() => {});
  }, []);

  useEffect(() => {
    // глобални – презареждаме подкатегории при смяна на категорията
    loadMeta(category).catch(() => {});
    setSubcategory("all");
  }, [category]);

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

  // Weekly Insights (/weekly/insights)
  async function loadWeeklyInsights() {
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

  // Top Movers (/weekly/trending) – ако липсва бекенд, масивът просто остава празен
  async function loadWeeklyMovers() {
    try {
      const qs = new URLSearchParams();
      qs.set("country", weeklyCountry);
      if (weeklyCategory !== "all") qs.set("category", weeklyCategory);
      if (weeklySubcategory !== "all") qs.set("subcategory", weeklySubcategory);
      const r = await fetch(`${API}/weekly/trending?${qs.toString()}`);
      const j = await r.json();
      setMoverRows(j.rows ?? []);
      if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
      if (j.week_start) setWeeklyStart(j.week_start);
      if (j.week_end) setWeeklyEnd(j.week_end);
    } catch {
      setMoverRows([]);
    }
  }

  async function loadHistory() {
    const qs = new URLSearchParams();
    if (historyCountry) qs.set("country", historyCountry);
    if (historyCategory !== "all") qs.set("category", historyCategory);
    if (historySubcategory !== "all") qs.set("subcategory", historySubcategory);
    if (historyStatus !== "all") qs.set("status", historyStatus);
    if (historyDate) qs.set("date", historyDate);
    const r = await fetch(`${API}/history?${qs.toString()}`);
    const j = await r.json();
    setHistoryRows(j.results ?? []);
    setHistoryDates(j.available_dates ?? []);
  }

  // Init by tab
  useEffect(() => {
    if (tab === "compare") loadCompare();
    else if (tab === "weekly") {
      if (weeklyInnerTab === "insights") loadWeeklyInsights();
      else loadWeeklyMovers();
    } else if (tab === "history") loadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  // Reactivity for filters
  useEffect(() => {
    if (tab === "compare") loadCompare();
  }, [country, category, subcategory, tab]);

  useEffect(() => {
    if (tab === "weekly") {
      if (weeklyInnerTab === "insights") loadWeeklyInsights();
      else loadWeeklyMovers();
    }
  }, [weeklyCountry, weeklyCategory, weeklySubcategory, weeklyStatus, weeklyInnerTab, tab]);

  // History subcategories meta
  useEffect(() => {
    async function loadHistorySubcats(cat: string) {
      const res = await fetch(`${API}/meta?category=${encodeURIComponent(cat)}`);
      const m = await res.json();
      setHistorySubcatOptions(m.subcategories ?? []);
    }
    if (historyCategory !== "all") loadHistorySubcats(historyCategory).catch(() => {});
    else setHistorySubcatOptions([]);
    setHistorySubcategory("all");
  }, [historyCategory]);

  useEffect(() => {
    if (tab === "history") loadHistory();
  }, [historyCountry, historyCategory, historySubcategory, historyStatus, historyDate, tab]);

  /* -------------------------- Derived / Sorting -------------------------- */

  const sortedCompare = useMemo(() => {
    const data = [...rows];
    data.sort((a, b) => {
      const va = a.current_rank ?? Number.POSITIVE_INFINITY;
      const vb = b.current_rank ?? Number.POSITIVE_INFINITY;
      return sortAsc ? va - vb : vb - va;
    });
    return data;
  }, [rows, sortAsc]);

  const sortedWeeklyInsights = useMemo(() => {
    // сортиране по статус (NEW преди RE-ENTRY) при клик по колоната
    const order = (s: "NEW" | "RE-ENTRY") => (s === "NEW" ? 0 : 1);
    const arr = [...weeklyRows];
    arr.sort((a, b) => {
      if (weeklySortByStatusAsc) return order(a.status) - order(b.status);
      else return order(b.status) - order(a.status);
    });
    return arr;
  }, [weeklyRows, weeklySortByStatusAsc]);

  /* -------------------------- Export helpers -------------------------- */

  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    qs.set("country", weeklyCountry);
    if (weeklyCategory !== "all") qs.set("category", weeklyCategory);
    if (weeklySubcategory !== "all") qs.set("subcategory", weeklySubcategory);
    if (weeklyStatus !== "all" && weeklyInnerTab === "insights") qs.set("status", weeklyStatus);
    qs.set("format", "csv");
    const path =
      weeklyInnerTab === "insights" ? "/weekly/insights" : "/weekly/trending";
    window.open(`${API}${path}?${qs.toString()}`, "_blank");
  }

  function exportCompareCSV() {
    const qs = new URLSearchParams();
    qs.set("limit", "50");
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    qs.set("format", "csv");
    window.open(`${API}/compare?${qs.toString()}`, "_blank");
  }

  function exportHistoryCSV() {
    const qs = new URLSearchParams();
    if (historyCountry) qs.set("country", historyCountry);
    if (historyCategory !== "all") qs.set("category", historyCategory);
    if (historySubcategory !== "all") qs.set("subcategory", historySubcategory);
    if (historyStatus !== "all") qs.set("status", historyStatus);
    if (historyDate) qs.set("date", historyDate);
    qs.set("export", "csv");
    window.open(`${API}/history?${qs.toString()}`, "_blank");
  }

  async function refreshDB() {
    try {
      const res = await fetch(`${API}/admin/refresh`);
      const j = await res.json();
      if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
      if (tab === "compare") await loadCompare();
      else if (tab === "weekly") {
        if (weeklyInnerTab === "insights") await loadWeeklyInsights();
        else await loadWeeklyMovers();
      } else await loadHistory();
      alert(j.message ?? "Refreshed");
    } catch {
      alert("Refresh failed.");
    }
  }

  /* ------------------------------- UI ------------------------------- */

  const T = tableStyles();

  return (
    <div
      style={{
        fontFamily: "Inter, system-ui, Arial",
        padding: 16,
        maxWidth: 1400,
        margin: "0 auto",
        background: "#fafafa",
      }}
    >
      <h1 style={{ margin: "6px 0 14px" }}>📊 App Store Dashboard</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 10, marginBottom: 14, flexWrap: "wrap" }}>
        <button
          style={{
            background: tab === "compare" ? "#007AFF" : "#e6e9ef",
            color: tab === "compare" ? "#fff" : "#111",
            padding: "8px 14px",
            borderRadius: 8,
            border: "1px solid #d9dce3",
          }}
          onClick={() => setTab("compare")}
        >
          Compare View
        </button>
        <button
          style={{
            background: tab === "weekly" ? "#007AFF" : "#e6e9ef",
            color: tab === "weekly" ? "#fff" : "#111",
            padding: "8px 14px",
            borderRadius: 8,
            border: "1px solid #d9dce3",
          }}
          onClick={() => setTab("weekly")}
        >
          Weekly View
        </button>
        <button
          style={{
            background: tab === "history" ? "#007AFF" : "#e6e9ef",
            color: tab === "history" ? "#fff" : "#111",
            padding: "8px 14px",
            borderRadius: 8,
            border: "1px solid #d9dce3",
          }}
          onClick={() => setTab("history")}
        >
          History View
        </button>
      </div>

      {/* Глобални филтри (за Compare) */}
      {sectionCard(
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
          <strong style={{ marginRight: 6 }}>Global filters (Compare)</strong>
          <select value={country} onChange={(e) => setCountry(e.target.value)}>
            {countries.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>

          <select value={category} onChange={(e) => setCategory(e.target.value)}>
            <option value="all">All categories</option>
            {categories.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>

          <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)}>
            <option value="all">All subcategories</option>
            {subcategories.map((s) => (
              <option key={s} value={s}>{s}</option>
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

          <button onClick={() => (tab === "compare" ? loadCompare() : tab === "weekly" ? (weeklyInnerTab === "insights" ? loadWeeklyInsights() : loadWeeklyMovers()) : loadHistory())}>
            Reload
          </button>

          <button onClick={refreshDB} style={{ display: "inline-flex", gap: 6 }}>
            🔄 Refresh DB
          </button>

          {tab === "compare" ? (
            <button
              onClick={exportCompareCSV}
              style={{ background: "#28a745", color: "white", padding: "6px 10px", borderRadius: 8 }}
            >
              Export CSV
            </button>
          ) : tab === "weekly" ? (
            <button
              onClick={exportWeeklyCSV}
              style={{ background: "#28a745", color: "white", padding: "6px 10px", borderRadius: 8 }}
            >
              Export CSV
            </button>
          ) : (
            <button
              onClick={exportHistoryCSV}
              style={{ background: "#28a745", color: "white", padding: "6px 10px", borderRadius: 8 }}
            >
              Export CSV
            </button>
          )}
        </div>
      )}

      {/* Last updated */}
      <div style={{ margin: "10px 0 14px", color: "#555" }}>
        Data last updated: <b>{latestSnapshot ?? "N/A"}</b>
      </div>

      {/* ------------------------------ Compare ------------------------------ */}
      {tab === "compare" &&
        sectionCard(
          <div style={{ overflowX: "auto" }}>
            <table style={T.table as any}>
              <thead>
                <tr>
                  <th style={T.th}>App</th>
                  <th style={T.th}>App ID</th>
                  <th style={T.th}>Country</th>
                  <th style={T.th}>Category</th>
                  <th style={T.th}>Subcategory</th>
                  <th
                    style={{ ...T.th, textAlign: "right", cursor: "pointer" }}
                    onClick={() => setSortAsc((s) => !s)}
                    title="Sort by current rank"
                  >
                    Current {sortAsc ? "↑" : "↓"}
                  </th>
                  <th style={{ ...T.th, textAlign: "right" }}>Previous (avg 7d)</th>
                  <th style={{ ...T.th, textAlign: "right" }}>Δ</th>
                  <th style={{ ...T.th, textAlign: "center" }}>Status</th>
                </tr>
              </thead>
              <tbody>
                {sortedCompare.map((r, i) => (
                  <tr
                    key={r.app_id + i}
                    style={{ background: i % 2 ? "#fcfcfc" : "#fff" }}
                  >
                    <td style={T.td}>{r.app_name}</td>
                    <td style={{ ...T.td, whiteSpace: "nowrap" }}>{r.app_id}</td>
                    <td style={T.td}>{r.country}</td>
                    <td style={T.td}>{r.category}</td>
                    <td style={T.td}>{r.subcategory ?? "—"}</td>
                    <td style={{ ...T.td, textAlign: "right" }}>
                      {r.current_rank ?? "—"}
                    </td>
                    <td style={{ ...T.td, textAlign: "right" }}>
                      {r.previous_rank ?? "—"}
                    </td>
                    <td
                      style={{
                        ...T.td,
                        textAlign: "right",
                        color:
                          (r.delta ?? 0) > 0
                            ? "green"
                            : (r.delta ?? 0) < 0
                            ? "#cc0000"
                            : undefined,
                      }}
                    >
                      {r.delta ?? "—"}
                    </td>
                    <td style={{ ...T.td, textAlign: "center" }}>
                      <StatusIcon row={r} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

      {/* ------------------------------ Weekly ------------------------------ */}
      {tab === "weekly" &&
        sectionCard(
          <div style={{ marginTop: 6 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
              <h3 style={{ margin: 0 }}>
                WEEKLY INSIGHTS —{" "}
                <span style={{ fontWeight: 400 }}>
                  (Week of {formatWeekRange(weeklyStart, weeklyEnd)})
                </span>
              </h3>

              {/* вътрешни табове като бутони */}
              <div style={{ marginLeft: "auto", display: "flex", gap: 6 }}>
                <button
                  onClick={() => setWeeklyInnerTab("insights")}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 8,
                    background:
                      weeklyInnerTab === "insights" ? "#007AFF" : "#e6e9ef",
                    color: weeklyInnerTab === "insights" ? "#fff" : "#111",
                    border: "1px solid #d9dce3",
                  }}
                >
                  🆕 New & Re-Entry
                </button>
                <button
                  onClick={() => setWeeklyInnerTab("movers")}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 8,
                    background:
                      weeklyInnerTab === "movers" ? "#007AFF" : "#e6e9ef",
                    color: weeklyInnerTab === "movers" ? "#fff" : "#111",
                    border: "1px solid #d9dce3",
                  }}
                >
                  📈 Top Movers
                </button>
              </div>
            </div>

            {/* вътрешни филтри */}
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 8,
                alignItems: "center",
                marginBottom: 12,
                background: "#f5f7fb",
                border: "1px solid #e7ebf3",
                padding: 8,
                borderRadius: 8,
              }}
            >
              <select
                value={weeklyCountry}
                onChange={(e) => setWeeklyCountry(e.target.value)}
              >
                {countries.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
              <select
                value={weeklyCategory}
                onChange={(e) => setWeeklyCategory(e.target.value)}
              >
                <option value="all">All categories</option>
                {categories.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
              <select
                value={weeklySubcategory}
                onChange={(e) => setWeeklySubcategory(e.target.value)}
              >
                <option value="all">All subcategories</option>
                {subcategories.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>

              {/* статус филтър има смисъл само в Insights таба */}
              {weeklyInnerTab === "insights" && (
                <select
                  value={weeklyStatus}
                  onChange={(e) => setWeeklyStatus(e.target.value as any)}
                >
                  <option value="all">All statuses</option>
                  <option value="NEW">NEW</option>
                  <option value="RE-ENTRY">RE-ENTRY</option>
                </select>
              )}

              <button
                onClick={() =>
                  weeklyInnerTab === "insights"
                    ? loadWeeklyInsights()
                    : loadWeeklyMovers()
                }
              >
                Reload
              </button>
              <button onClick={exportWeeklyCSV}>Export CSV</button>
            </div>

            {weeklyInnerTab === "insights" ? (
              <>
                <div style={{ marginBottom: 6, color: "#555" }}>
                  Showing: <b>{weeklyStatus === "all" ? "All" : weeklyStatus}</b>
                  {weeklyCounts && (
                    <span style={{ marginLeft: 12 }}>
                      NEW: <b>{weeklyCounts.NEW}</b> • RE-ENTRY:{" "}
                      <b>{weeklyCounts.RE_ENTRY}</b>
                    </span>
                  )}
                </div>

                <div style={{ overflowX: "auto" }}>
                  <table style={T.table as any}>
                    <thead>
                      <tr>
                        <th style={{ ...T.th, textAlign: "right" }}>Rank</th>
                        <th style={T.th}>App</th>
                        <th style={T.th}>App ID</th>
                        <th style={T.th}>Country</th>
                        <th style={T.th}>Category</th>
                        <th style={T.th}>Subcategory</th>
                        <th
                          style={{ ...T.th, cursor: "pointer" }}
                          onClick={() =>
                            setWeeklySortByStatusAsc((s) => !s)
                          }
                          title="Sort by Status"
                        >
                          Status {weeklySortByStatusAsc ? "↑" : "↓"}
                        </th>
                        <th style={T.th}>Δ Rank (vs last week)</th>
                        <th style={T.th}>First seen</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedWeeklyInsights.length === 0 && (
                        <tr>
                          <td
                            colSpan={9}
                            style={{
                              textAlign: "center",
                              padding: 12,
                              color: "#777",
                            }}
                          >
                            No data for selected filters.
                          </td>
                        </tr>
                      )}
                      {sortedWeeklyInsights.map((r, i) => (
                        <tr
                          key={r.app_id + i}
                          style={{
                            background:
                              rowBgByStatus(r.status) ||
                              (i % 2 ? "#fcfcfc" : "#fff"),
                          }}
                        >
                          <td style={{ ...T.td, textAlign: "right" }}>
                            {r.rank ?? "—"}
                          </td>
                          <td style={T.td}>{r.app_name}</td>
                          <td style={T.td}>{r.app_id}</td>
                          <td style={T.td}>{r.country}</td>
                          <td style={T.td}>{r.category ?? "—"}</td>
                          <td style={T.td}>{r.subcategory ?? "—"}</td>
                          <td style={T.td}>{statusChip(r.status)}</td>
                          <td style={T.td}>
                            {typeof r.rank_delta === "number"
                              ? r.rank_delta > 0
                                ? `↑ +${r.rank_delta}`
                                : r.rank_delta < 0
                                ? `↓ ${r.rank_delta}`
                                : "—"
                              : r.last_week_rank != null && r.rank != null
                              ? (() => {
                                  const d = (r.last_week_rank ?? 0) - (r.rank ?? 0);
                                  return d > 0 ? `↑ +${d}` : d < 0 ? `↓ ${d}` : "—";
                                })()
                              : "—"}
                          </td>
                          <td style={T.td}>{r.first_seen_date}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              /* Top Movers */
              <div style={{ overflowX: "auto" }}>
                <table style={T.table as any}>
                  <thead>
                    <tr>
                      <th style={{ ...T.th, textAlign: "right" }}>Rank</th>
                      <th style={T.th}>Δ Rank (vs last week)</th>
                      <th style={T.th}>App</th>
                      <th style={T.th}>App ID</th>
                      <th style={T.th}>Country</th>
                      <th style={T.th}>Category</th>
                      <th style={T.th}>Subcategory</th>
                    </tr>
                  </thead>
                  <tbody>
                    {moverRows.length === 0 && (
                      <tr>
                        <td colSpan={7} style={{ textAlign: "center", padding: 12, color: "#777" }}>
                          No data (trending endpoint).
                        </td>
                      </tr>
                    )}
                    {[...moverRows]
                      .sort((a, b) => (b.rank_delta ?? 0) - (a.rank_delta ?? 0))
                      .map((r, i) => (
                        <tr key={r.app_id + i} style={{ background: i % 2 ? "#fcfcfc" : "#fff" }}>
                          <td style={{ ...T.td, textAlign: "right" }}>{r.rank ?? "—"}</td>
                          <td style={T.td}>
                            {typeof r.rank_delta === "number"
                              ? r.rank_delta > 0
                                ? `↑ +${r.rank_delta}`
                                : r.rank_delta < 0
                                ? `↓ ${r.rank_delta}`
                                : "—"
                              : "—"}
                          </td>
                          <td style={T.td}>{r.app_name}</td>
                          <td style={T.td}>{r.app_id}</td>
                          <td style={T.td}>{r.country}</td>
                          <td style={T.td}>{r.category ?? "—"}</td>
                          <td style={T.td}>{r.subcategory ?? "—"}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

      {/* ------------------------------ History ------------------------------ */}
      {tab === "history" &&
        sectionCard(
          <div style={{ marginTop: 6 }}>
            <h3 style={{ marginTop: 0 }}>📆 History View (Re-Entry Tracker)</h3>

            {/* History Filters */}
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 8,
                marginBottom: 12,
                background: "#f5f7fb",
                border: "1px solid #e7ebf3",
                padding: 8,
                borderRadius: 8,
              }}
            >
              <select
                value={historyCountry}
                onChange={(e) => setHistoryCountry(e.target.value)}
              >
                {countries.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>

              <select
                value={historyCategory}
                onChange={(e) => setHistoryCategory(e.target.value)}
              >
                <option value="all">All categories</option>
                {categories.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>

              <select
                value={historySubcategory}
                onChange={(e) => setHistorySubcategory(e.target.value)}
              >
                <option value="all">All subcategories</option>
                {(historySubcatOptions.length ? historySubcatOptions : subcategories).map(
                  (s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  )
                )}
              </select>

              <select
                value={historyStatus}
                onChange={(e) => setHistoryStatus(e.target.value)}
              >
                <option value="all">All statuses</option>
                <option value="NEW">NEW</option>
                <option value="DROPPED">DROPPED</option>
                <option value="RE-ENTRY">RE-ENTRY</option>
              </select>

              <select
                value={historyDate}
                onChange={(e) => setHistoryDate(e.target.value)}
              >
                <option value="">All dates</option>
                {historyDates.map((d) => (
                  <option key={d} value={d}>
                    {d}
                  </option>
                ))}
              </select>

              <button
                onClick={() => {
                  setHistoryCategory("all");
                  setHistorySubcategory("all");
                  setHistoryStatus("all");
                  setHistoryDate("");
                }}
              >
                Reset
              </button>
              <button onClick={loadHistory}>Reload</button>

              <button
                onClick={exportHistoryCSV}
                style={{
                  background: "#28a745",
                  color: "white",
                  padding: "6px 10px",
                  borderRadius: 8,
                }}
              >
                Export CSV
              </button>
            </div>

            {/* Table */}
            <div style={{ overflowX: "auto" }}>
              <table style={T.table as any}>
                <thead>
                  <tr>
                    <th style={T.th}>Date</th>
                    <th style={T.th}>Country</th>
                    <th style={T.th}>App</th>
                    <th style={T.th}>App ID</th>
                    <th style={T.th}>Status</th>
                    <th style={{ ...T.th, textAlign: "right" }}>Rank</th>
                    <th style={T.th}>Replaced ↔ By</th>
                    <th style={T.th}>Replaced Now</th>
                    <th style={{ ...T.th, textAlign: "right" }}>Prev Rank</th>
                    <th style={{ ...T.th, textAlign: "right" }}>Curr Rank</th>
                  </tr>
                </thead>
                <tbody>
                  {historyRows.length === 0 && (
                    <tr>
                      <td
                        colSpan={10}
                        style={{ padding: 12, textAlign: "center", color: "#777" }}
                      >
                        No data for selected filters.
                      </td>
                    </tr>
                  )}
                  {historyRows.map((r: any, i: number) => {
                    const replacedName =
                      r.status === "NEW"
                        ? r.replaced_app_name
                        : r.status === "DROPPED"
                        ? r.replaced_by_app_name
                        : "";
                    const replacedNow =
                      r.status === "NEW"
                        ? r.replaced_current_rank
                          ? `now #${r.replaced_current_rank}`
                          : r.replaced_status === "DROPPED"
                          ? "dropped"
                          : ""
                        : r.status === "DROPPED" && r.replaced_by_rank
                        ? `rank #${r.replaced_by_rank}`
                        : "";
                    return (
                      <tr key={(r.app_id ?? "x") + i} style={{ background: i % 2 ? "#fcfcfc" : "#fff" }}>
                        <td style={T.td}>{r.date}</td>
                        <td style={T.td}>{r.country}</td>
                        <td style={T.td}>{r.app_name}</td>
                        <td style={{ ...T.td, whiteSpace: "nowrap" }}>{r.app_id}</td>
                        <td style={{ ...T.td }}>
                          {statusChip(r.status)}
                        </td>
                        <td style={{ ...T.td, textAlign: "right" }}>{r.rank ?? "—"}</td>
                        <td style={T.td}>{replacedName || "—"}</td>
                        <td style={T.td}>{replacedNow || "—"}</td>
                        <td style={{ ...T.td, textAlign: "right" }}>{r.previous_rank ?? "—"}</td>
                        <td style={{ ...T.td, textAlign: "right" }}>{r.current_rank ?? "—"}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
    </div>
  );
}
