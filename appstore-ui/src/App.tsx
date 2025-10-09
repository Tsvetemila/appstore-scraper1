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
  const [tab, setTab] = useState<"compare" | "weekly" | "history">("compare");

  // meta
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<string | null>(null);

  // filters
  const [country, setCountry] = useState<string>("US");
  const [category, setCategory] = useState<string>("all");
  const [subcategory, setSubcategory] = useState<string>("all");

  // data
  const [rows, setRows] = useState<CompareRow[]>([]);
  const [weekly, setWeekly] = useState<{ new: WeeklyRow[]; dropped: WeeklyRow[] }>({ new: [], dropped: [] });

  // history data + filters
  const [historyRows, setHistoryRows] = useState<any[]>([]);
  const [historyDates, setHistoryDates] = useState<string[]>([]);
  const [historyDate, setHistoryDate] = useState<string>("");
  const [historyStatus, setHistoryStatus] = useState<string>("all");

  // sort state (compare)
  const [sortAsc, setSortAsc] = useState<boolean>(true);

  // ---------- load META once ----------
  async function loadMeta(selectedCategory?: string) {
    const res = await fetch(
      `${API}/meta${selectedCategory ? `?category=${encodeURIComponent(selectedCategory)}` : ""}`
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

  // reload subcategories when category changes
  useEffect(() => {
    loadMeta(category).catch(() => {});
    // reset subcat when category changes
    setSubcategory("all");
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
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
  }

  async function loadWeekly() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/reports/weekly?${qs.toString()}`);
    const j = await r.json();
    setWeekly({ new: j.new ?? [], dropped: j.dropped ?? [] });
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
  }

  async function loadHistory() {
    const qs = new URLSearchParams();
    if (country) qs.set("country", country);
    if (historyStatus !== "all") qs.set("status", historyStatus);
    if (historyDate) qs.set("date", historyDate);
    const r = await fetch(`${API}/history?${qs.toString()}`);
    const j = await r.json();
    setHistoryRows(j.results ?? []);
    setHistoryDates(j.available_dates ?? []);
  }

  useEffect(() => {
    if (!country) return;
    if (tab === "compare") loadCompare();
    else if (tab === "weekly") loadWeekly();
    else if (tab === "history") loadHistory();
  }, [country, category, subcategory, tab, historyDate, historyStatus]);

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
    qs.set("format", "csv");
    window.open(`${API}/reports/weekly?${qs.toString()}`, "_blank");
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
    if (country) qs.set("country", country);
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
      // reload current tab after refresh
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
    <div
      style={{
        fontFamily: "Inter, system-ui, Arial",
        padding: 16,
        maxWidth: 1400,
        margin: "0 auto",
      }}
    >
      <h1>📊 App Store Dashboard</h1>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, flexWrap: "wrap" }}>
        <button
          style={{
            background: tab === "compare" ? "#007AFF" : "#dadde1",
            color: tab === "compare" ? "#fff" : "#111",
            padding: "6px 14px",
            borderRadius: 6,
          }}
          onClick={() => setTab("compare")}
        >
          Compare View
        </button>
        <button
          style={{
            background: tab === "weekly" ? "#007AFF" : "#dadde1",
            color: tab === "weekly" ? "#fff" : "#111",
            padding: "6px 14px",
            borderRadius: 6,
          }}
          onClick={() => setTab("weekly")}
        >
          Weekly Report
        </button>
        <button
          style={{
            background: tab === "history" ? "#007AFF" : "#dadde1",
            color: tab === "history" ? "#fff" : "#111",
            padding: "6px 14px",
            borderRadius: 6,
          }}
          onClick={() => setTab("history")}
        >
          History View
        </button>
      </div>

      {/* Filters + actions */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 10 }}>
        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          {countries.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select value={category} onChange={(e) => setCategory(e.target.value)}>
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

        <button onClick={() => (tab === "compare" ? loadCompare() : tab === "weekly" ? loadWeekly() : loadHistory())}>
          Reload
        </button>

        <button onClick={refreshDB} style={{ display: "inline-flex", gap: 6 }}>
          🔄 Refresh DB
        </button>

        {/* Export per view */}
        {tab === "compare" ? (
          <button onClick={exportCompareCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>
            Export CSV
          </button>
        ) : tab === "weekly" ? (
          <button onClick={exportWeeklyCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>
            Export CSV
          </button>
        ) : (
          <button onClick={exportHistoryCSV} style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}>
            Export CSV
          </button>
        )}
      </div>

      {/* Last updated */}
      <div style={{ marginBottom: 8, color: "#555" }}>
        Data last updated: <b>{latestSnapshot ?? "N/A"}</b>
      </div>

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
                <th
                  style={{ textAlign: "right", padding: 8, cursor: "pointer", whiteSpace: "nowrap" }}
                  onClick={() => setSortAsc((s) => !s)}
                  title="Sort by current rank"
                >
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
          <div style={{ flex: 1, minWidth: 420 }}>
            <h3>🆕 New Entries ({weekly.new.length})</h3>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "right", padding: 6 }}>Rank</th>
                    <th style={{ textAlign: "left", padding: 6 }}>App</th>
                    <th style={{ textAlign: "left", padding: 6 }}>App ID</th>
                    <th style={{ textAlign: "left", padding: 6 }}>Developer</th>
                  </tr>
                </thead>
                <tbody>
                  {[...weekly.new].sort((a, b) => (a.rank ?? 999) - (b.rank ?? 999)).map((a, i) => (
                    <tr key={a.app_id + i}>
                      <td style={{ textAlign: "right", padding: 6 }}>{a.rank ?? "—"}</td>
                      <td style={{ padding: 6 }}>{a.app_name}</td>
                      <td style={{ padding: 6, whiteSpace: "nowrap" }}>{a.app_id}</td>
                      <td style={{ padding: 6 }}>{a.developer_name}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div style={{ flex: 1, minWidth: 420 }}>
            <h3>❌ Dropped ({weekly.dropped.length})</h3>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "right", padding: 6 }}>Rank</th>
                    <th style={{ textAlign: "left", padding: 6 }}>App</th>
                    <th style={{ textAlign: "left", padding: 6 }}>App ID</th>
                    <th style={{ textAlign: "left", padding: 6 }}>Developer</th>
                  </tr>
                </thead>
                <tbody>
                  {[...weekly.dropped].sort((a, b) => (a.rank ?? 999) - (b.rank ?? 999)).map((a, i) => (
                    <tr key={a.app_id + i}>
                      <td style={{ textAlign: "right", padding: 6 }}>{a.rank ?? "—"}</td>
                      <td style={{ padding: 6 }}>{a.app_name}</td>
                      <td style={{ padding: 6, whiteSpace: "nowrap" }}>{a.app_id}</td>
                      <td style={{ padding: 6 }}>{a.developer_name}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* History View */}
      {tab === "history" && (
        <div style={{ marginTop: 20 }}>
          <h3>📆 History View (Re-Entry Tracker)</h3>

          {/* History Filters */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 12 }}>
            <select value={historyStatus} onChange={(e) => setHistoryStatus(e.target.value)}>
              <option value="all">All statuses</option>
              <option value="NEW">NEW</option>
              <option value="DROPPED">DROPPED</option>
              <option value="MOVED">MOVED</option>
              <option value="RE-ENTRY">RE-ENTRY</option>
            </select>

            <select value={historyDate} onChange={(e) => setHistoryDate(e.target.value)}>
              <option value="">All dates</option>
              {historyDates.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>

            <button onClick={loadHistory}>Reload</button>

            <button
              onClick={exportHistoryCSV}
              style={{ background: "#28a745", color: "white", padding: "4px 10px", borderRadius: 6 }}
            >
              Export CSV
            </button>
          </div>

          {/* Table */}
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #eee" }}>
              <thead style={{ background: "#f7f7f7" }}>
                <tr>
                  <th style={{ textAlign: "left", padding: 8 }}>Date</th>
                  <th style={{ textAlign: "left", padding: 8 }}>App</th>
                  <th style={{ textAlign: "left", padding: 8 }}>App ID</th>
                  <th style={{ textAlign: "left", padding: 8 }}>Status</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Rank</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Prev Rank</th>
                  <th style={{ textAlign: "right", padding: 8 }}>Curr Rank</th>
                </tr>
              </thead>
              <tbody>
                {historyRows.length === 0 && (
                  <tr>
                    <td colSpan={7} style={{ padding: 8, textAlign: "center", color: "#777" }}>
                      No data for selected filters.
                    </td>
                  </tr>
                )}
                {historyRows.map((r: any, i: number) => (
                  <tr key={(r.app_id ?? "x") + i} style={{ borderBottom: "1px solid #f0f0f0" }}>
                    <td style={{ padding: 8 }}>{r.date}</td>
                    <td style={{ padding: 8 }}>{r.app_name}</td>
                    <td style={{ padding: 8, whiteSpace: "nowrap" }}>{r.app_id}</td>
                    <td
                      style={{
                        padding: 8,
                        color:
                          r.status === "NEW"
                            ? "#007AFF"
                            : r.status === "DROPPED"
                            ? "#cc0000"
                            : r.status === "RE-ENTRY"
                            ? "green"
                            : "#555",
                      }}
                    >
                      {r.status}
                    </td>
                    <td style={{ padding: 8, textAlign: "right" }}>{r.rank ?? "—"}</td>
                    <td style={{ padding: 8, textAlign: "right" }}>{r.previous_rank ?? "—"}</td>
                    <td style={{ padding: 8, textAlign: "right" }}>{r.current_rank ?? "—"}</td>
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
