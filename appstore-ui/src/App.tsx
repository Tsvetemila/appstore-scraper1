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
  developer_name: string; // присъства в API, но вече не го визуализираме
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
  const [tab, setTab] = useState<"compare" | "weekly" | "history">("compare");

  // META (глобални списъци)
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<string | null>(null);

  // глобални филтри (за Compare/Weekly)
  const [country, setCountry] = useState<string>("US");
  const [category, setCategory] = useState<string>("all");
  const [subcategory, setSubcategory] = useState<string>("all");

  // данни
  const [rows, setRows] = useState<CompareRow[]>([]);
  // Weekly Insights state (internal to the tab)
  const [weeklyRows, setWeeklyRows] = useState<WeeklyInsightRow[]>([]);
  const [weeklyStart, setWeeklyStart] = useState<string | null>(null);
  const [weeklyEnd, setWeeklyEnd] = useState<string | null>(null);
  const [weeklyCounts, setWeeklyCounts] = useState<{ NEW: number; RE_ENTRY: number } | null>(null);
  const [weeklyStatus, setWeeklyStatus] = useState<"all" | "NEW" | "RE-ENTRY">("all");
  const [weeklyCountry, setWeeklyCountry] = useState<string>(country);
  const [weeklyCategory, setWeeklyCategory] = useState<string>(category);
  const [weeklySubcategory, setWeeklySubcategory] = useState<string>(subcategory);
  // History – отделни филтри (за да не пипат другите табове)
  const [historyRows, setHistoryRows] = useState<any[]>([]);
  const [historyDates, setHistoryDates] = useState<string[]>([]);
  const [historyDate, setHistoryDate] = useState<string>("");
  const [historyStatus, setHistoryStatus] = useState<string>("all");
  const [historyCountry, setHistoryCountry] = useState<string>("US");
  const [historyCategory, setHistoryCategory] = useState<string>("all");
  const [historySubcategory, setHistorySubcategory] = useState<string>("all");
  const [historySubcatOptions, setHistorySubcatOptions] = useState<string[]>([]);

  // sort state (compare)
  const [sortAsc, setSortAsc] = useState<boolean>(true);

  // ---------- load META once ----------
  async function loadMeta(selectedCategory?: string) {
    const res = await fetch(`${API}/meta${selectedCategory ? `?category=${encodeURIComponent(selectedCategory)}` : ""}`);
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
    if ((m.countries ?? []).length > 0 && !countries.length) setCountry(m.countries[0]);
  }
  useEffect(() => { loadMeta().catch(() => {}); }, []);

  // reload subcategories when category changes (глобални)
  useEffect(() => {
    loadMeta(category).catch(() => {});
    setSubcategory("all");
  }, [category]);

  // ---------- data loaders ----------
  async function loadCompare() {
    const qs = new URLSearchParams();
    qs.set("limit", "50");
    qs.set("country", weeklyCountry);
    if (weeklyCategory !== "all") qs.set("category", weeklyCategory);
    if (weeklySubcategory !== "all") qs.set("subcategory", weeklySubcategory);
    const r = await fetch(`${API}/compare?${qs.toString()}`);
    const j = await r.json();
    setRows(j.results ?? []);
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
  }

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
  /reports/weekly?${qs.toString()}`);
    const j = await r.json();
    setWeekly({ new: j.new ?? [], dropped: j.dropped ?? [] });
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
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

  // при смяна на таб/филтри зареждаме съответните данни
  useEffect(() => {
    if (tab === "compare") loadCompare();
    else if (tab === "weekly") loadWeekly();
    else if (tab === "history") loadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  // Compare се рефрешва при смяна на неговите филтри
  useEffect(() => { if (tab === "compare") loadCompare(); }, [country, category, subcategory, tab]);

  // Weekly се рефрешва при смяна на неговите филтри
  useEffect(() => { if (tab === "weekly") loadWeekly(); }, [weeklyCountry, weeklyCategory, weeklySubcategory, weeklyStatus, tab]);
// History – мета за подкатегории (само за History филтъра)
  useEffect(() => {
    async function loadHistorySubcats(cat: string) {
      const res = await fetch(`${API}/meta?category=${encodeURIComponent(cat)}`);
      const m = await res.json();
      setHistorySubcatOptions(m.subcategories ?? []);
    }
    if (historyCategory !== "all") loadHistorySubcats(historyCategory).catch(() => {});
    else setHistorySubcatOptions([]); // „All subcategories“
    setHistorySubcategory("all");
  }, [historyCategory]);

  // при смяна на някой от History филтрите — auto reload
  useEffect(() => { if (tab === "history") loadHistory(); }, [historyCountry, historyCategory, historySubcategory, historyStatus, historyDate, tab]);

  // derived – sorted compare rows
  const sortedRows = useMemo(() => {
    const data = [...rows];
    data.sort((a, b) => {
      const va = a.current_rank ?? Number.POSITIVE_INFINITY;
      const vb = b.current_rank ?? Number.POSITIVE_INFINITY;
      return sortAsc ? va - vb : vb - va;
    });
    return data;
  }, [rows, sortAsc]);

  // export helpers
  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    if (weeklyStatus !== "all") qs.set("status", weeklyStatus);
    qs.set("format", "csv");
    window.open(`${API}/weekly/insights?${qs.toString()}`, "_blank");
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
      else if (tab === "weekly") await loadWeekly();
      else await loadHistory();
      alert(j.message ?? "Refreshed");
    } catch {
      alert("Refresh failed.");
    }
  }

  // ---------- UI ----------
  return (
    <div style={{ fontFamily: "Inter, system-ui, Arial", padding: 16, maxWidth: 1400, margin: "0 auto" }}>
      <h1>📊 App Store Dashboard</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <button
          style={{ background: tab === "compare" ? "#007AFF" : "#dadde1", color: tab === "compare" ? "#fff" : "#111", padding: "6px 14px", borderRadius: 6 }}
          onClick={() => setTab("compare")}
        >
          Compare View
        </button>
        <button
          style={{ background: tab === "weekly" ? "#007AFF" : "#dadde1", color: tab === "weekly" ? "#fff" : "#111", padding: "6px 14px", borderRadius: 6 }}
          onClick={() => setTab("weekly")}
        >
          Weekly Report
        </button>
        <button
          style={{ background: tab === "history" ? "#007AFF" : "#dadde1", color: tab === "history" ? "#fff" : "#111", padding: "6px 14px", borderRadius: 6 }}
          onClick={() => setTab("history")}
        >
          History View
        </button>
      </div>

      {/* Глобални филтри (за Compare/Weekly) */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 10 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          {countries.map((c) => (<option key={c} value={c}>{c}</option>))}
        </select>

        <select value={category} onChange={(e) => setCategory(e.target.value)}>
          <option value="all">All categories</option>
          {categories.map((c) => (<option key={c} value={c}>{c}</option>))}
        </select>

        <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)}>
          <option value="all">All subcategories</option>
          {subcategories.map((s) => (<option key={s} value={s}>{s}</option>))}
        </select>

        <button onClick={() => { setCategory("all"); setSubcategory("all"); }}>Reset</button>
        <button onClick={() => (tab === "compare" ? loadCompare() : tab === "weekly" ? loadWeekly() : loadHistory())}>Reload</button>

        <button onClick={refreshDB} style={{ display: "inline-flex", gap: 6 }}>🔄 Refresh DB</button>

        {tab === "compare" ? (
          <button onClick={exportCompareCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>Export CSV</button>
        ) : tab === "weekly" ? (
          <button onClick={exportWeeklyCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>Export CSV</button>
        ) : (
          <button onClick={exportHistoryCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>Export CSV</button>
        )}
      </div>

      {/* Last updated */}
      <div style={{ marginBottom: 8, color: "#555" }}>Data last updated: <b>{latestSnapshot ?? "N/A"}</b></div>

      {/* Compare View */}
      {tab === "compare" && (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
            <thead style={{ background: "#f7f7f7" }}>
              <tr>
                <th style={{ textAlign: "left", padding: 8 }}>App</th>
                <th style={{ textAlign: "left", padding: 8 }}>App ID</th>
                <th style={{ textAlign: "left", padding: 8 }}>Country</th>
                <th style={{ textAlign: "left", padding: 8 }}>Category</th>
                <th style={{ textAlign: "left", padding: 8 }}>Subcategory</th>
                <th style={{ textAlign: "right", padding: 8, cursor: "pointer", whiteSpace: "nowrap" }}
                    onClick={() => setSortAsc((s) => !s)} title="Sort by current rank">
                  Current {sortAsc ? "↑" : "↓"}
                </th>
                <th style={{ textAlign: "right", padding: 8, whiteSpace: "nowrap" }}>Previous (avg 7d)</th>
                <th style={{ textAlign: "right", padding: 8 }}>Δ</th>
                <th style={{ textAlign: "center", padding: 8 }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((r, i) => (
                <tr key={r.app_id + i} style={{ borderBottom: "1px solid #f0f0f0" }}>
                  <td style={{ padding: 8 }}>{r.app_name}</td>
                  <td style={{ padding: 8, whiteSpace: "nowrap" }}>{r.app_id}</td>
                  <td style={{ padding: 8 }}>{r.country}</td>
                  <td style={{ padding: 8 }}>{r.category}</td>
                  <td style={{ padding: 8 }}>{r.subcategory ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "right" }}>{r.current_rank ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "right" }}>{r.previous_rank ?? "—"}</td>
                  <td style={{ padding: 8, textAlign: "right", color: (r.delta ?? 0) > 0 ? "green" : (r.delta ?? 0) < 0 ? "red" : undefined }}>
                    {r.delta ?? "—"}
                  </td>
                  <td style={{ padding: 8, textAlign: "center" }}><StatusIcon row={r} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Weekly Report View (без Developer колона) */}
      {tab === "weekly" && (
        <div style={{ marginTop: 10 }}>
          <h3 style={{ marginBottom: 8 }}>WEEKLY INSIGHTS — <span style={{ fontWeight: 400 }}>(Week of {formatWeekRange(weeklyStart, weeklyEnd)})</span></h3>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 12 }}>
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
            <button onClick={loadWeekly}>Reload</button>
            <button onClick={exportWeeklyCSV}>Export CSV</button>
          </div>
          <div style={{ marginBottom: 6, color: "#555" }}>
            Showing: <b>{weeklyStatus === "all" ? "All" : weeklyStatus}</b>{weeklyCounts && (<span style={{ marginLeft: 12 }}>NEW: <b>{weeklyCounts.NEW}</b> • RE-ENTRY: <b>{weeklyCounts.RE_ENTRY}</b></span>)}
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
              <thead style={{ background: "#f7f7f7" }}>
                <tr>
                  <th style={{ textAlign: "right", padding: 8 }}>Rank</th>
                  <th style={{ textAlign: "left", padding: 8 }}>App</th>
                  <th style={{ textAlign: "left", padding: 8 }}>App ID</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Country</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Category</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Subcategory</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Status</th>
                  <th style={{ textAlign: "left", padding: 8 }}>First seen</th>
                </tr>
              </thead>
              <tbody>
                {weeklyRows.length === 0 && (<tr><td colSpan={8} style={{ textAlign: "center", padding: 8, color: "#777" }}>No data for selected filters.</td></tr>)}
                {[...weeklyRows].sort((a, b) => (a.rank ?? 999) - (b.rank ?? 999)).map((r, i) => (
                  <tr key={r.app_id + i}>
                    <td style={{ textAlign: "right", padding: 8 }}>{r.rank ?? "—"}</td>
                    <td style={{ padding: 8 }}>{r.app_name}</td>
                    <td style={{ padding: 8 }}>{r.app_id}</td>
                    <td style={{ padding: 8 }}>{r.country}</td>
                    <td style={{ padding: 8 }}>{r.category ?? "—"}</td>
                    <td style={{ padding: 8 }}>{r.subcategory ?? "—"}</td>
                    <td style={{ padding: 8, color: r.status === "NEW" ? "#007AFF" : "green" }}>{r.status}</td>
                    <td style={{ padding: 8 }}>{r.first_seen_date}</td>
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

          {/* History Filters (самостоятелни) */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 12 }}>
            <select value={historyCountry} onChange={(e) => setHistoryCountry(e.target.value)}>
              {countries.map((c) => (<option key={c} value={c}>{c}</option>))}
            </select>

            <select value={historyCategory} onChange={(e) => setHistoryCategory(e.target.value)}>
              <option value="all">All categories</option>
              {categories.map((c) => (<option key={c} value={c}>{c}</option>))}
            </select>

            <select value={historySubcategory} onChange={(e) => setHistorySubcategory(e.target.value)}>
              <option value="all">All subcategories</option>
              {(historySubcatOptions.length ? historySubcatOptions : subcategories).map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>

            <select value={historyStatus} onChange={(e) => setHistoryStatus(e.target.value)}>
              <option value="all">All statuses</option>
              <option value="NEW">NEW</option>
              <option value="DROPPED">DROPPED</option>
              <option value="RE-ENTRY">RE-ENTRY</option>
            </select>

            <select value={historyDate} onChange={(e) => setHistoryDate(e.target.value)}>
              <option value="">All dates</option>
              {historyDates.map((d) => (<option key={d} value={d}>{d}</option>))}
            </select>

            <button onClick={() => { setHistoryCategory("all"); setHistorySubcategory("all"); setHistoryStatus("all"); setHistoryDate(""); }}>
              Reset
            </button>
            <button onClick={loadHistory}>Reload</button>

            <button onClick={exportHistoryCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>
              Export CSV
            </button>
          </div>

          {/* Table */}
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
              <thead style={{ background: "#f7f7f7" }}>
                <tr>
                  <th style={{ textAlign: "left", padding: 8 }}>Date</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Country</th>
                  <th style={{ textAlign: "left", padding: 8 }}>App</th>
                  <th style={{ textAlign: "left", padding: 8 }}>App ID</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Status</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Rank</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Replaced ↔ By</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Replaced Now</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Prev Rank</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Curr Rank</th>
                </tr>
              </thead>
              <tbody>
                {historyRows.length === 0 && (
                  <tr>
                    <td colSpan={9} style={{ padding: 8, textAlign: "center", color: "#777" }}>
                      No data for selected filters.
                    </td>
                  </tr>
                )}
                {historyRows.map((r: any, i: number) => {
                  const replacedName =
                    r.status === "NEW" ? r.replaced_app_name :
                    r.status === "DROPPED" ? r.replaced_by_app_name : "";
                  const replacedNow =
                    r.status === "NEW"
                      ? (r.replaced_current_rank ? `now #${r.replaced_current_rank}` : (r.replaced_status === "DROPPED" ? "dropped" : ""))
                      : (r.status === "DROPPED" && r.replaced_by_rank ? `rank #${r.replaced_by_rank}` : "");
                  return (
                    <tr key={(r.app_id ?? "x") + i} style={{ borderBottom: "1px solid #f0f0f0" }}>
                      <td style={{ padding: 8 }}>{r.date}</td>
                      <td style={{ padding: 8 }}>{r.country}</td>
                      <td style={{ padding: 8 }}>{r.app_name}</td>
                      <td style={{ padding: 8, whiteSpace: "nowrap" }}>{r.app_id}</td>
                      <td style={{ padding: 8, color:
                        r.status === "NEW" ? "#007AFF" :
                        r.status === "DROPPED" ? "#cc0000" :
                        r.status === "RE-ENTRY" ? "green" : "#555"
                      }}>
                        {r.status}
                      </td>
                      <td style={{ padding: 8, textAlign: "right" }}>{r.rank ?? "—"}</td>
                      <td style={{ padding: 8 }}>{replacedName || "—"}</td>
                      <td style={{ padding: 8 }}>{replacedNow || "—"}</td>
                      <td style={{ padding: 8, textAlign: "right" }}>{r.previous_rank ?? "—"}</td>
                      <td style={{ padding: 8, textAlign: "right" }}>{r.current_rank ?? "—"}</td>
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
