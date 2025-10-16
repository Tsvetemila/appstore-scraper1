import React, { useEffect, useMemo, useState } from "react";

const API = (import.meta as any)?.env?.VITE_API_URL || "https://appstore-api.onrender.com";

/* ---------- UI helpers (същия стил) ---------- */
const box = {
  background: "#f7f9fc",
  border: "1px solid #e7ecf3",
  borderRadius: 12,
  padding: 14,
  boxShadow: "0 1px 2px rgba(16,24,40,.04)",
};
const btn = (active = false) => ({
  background: active ? "#007AFF" : "#e5e7eb",
  color: active ? "#fff" : "#111",
  padding: "8px 14px",
  borderRadius: 8,
  border: "1px solid rgba(0,0,0,0.05)",
  cursor: "pointer",
  fontWeight: 600 as const,
});
const btnLite = {
  background: "#eef2ff",
  color: "#1f2937",
  padding: "6px 12px",
  borderRadius: 8,
  border: "1px solid #e5e7eb",
  cursor: "pointer",
  fontWeight: 600 as const,
};
const btnGreen = {
  background: "#2ea44f",
  color: "#fff",
  padding: "8px 14px",
  borderRadius: 8,
  border: "1px solid rgba(0,0,0,0.05)",
  cursor: "pointer",
  fontWeight: 600 as const,
};
const pill = (bg: string, color = "#fff") => ({
  background: bg,
  color,
  padding: "2px 8px",
  borderRadius: 999,
  fontSize: 12,
  fontWeight: 700 as const,
});
function formatWeekLabel(a?: string | null, b?: string | null) {
  if (!a || !b) return "WEEKLY INSIGHTS";
  const s = new Date(a);
  const e = new Date(b);
  const fmt = (d: Date, withYear = false) =>
    d.toLocaleDateString("en-US", { month: "short", day: "numeric", ...(withYear ? { year: "numeric" } : {}) });
  return `WEEKLY INSIGHTS — (Week of ${fmt(s)}–${fmt(e, true)})`;
}

/* ---------- Types ---------- */
type CompareRow = {
  app_id: string;
  app_name: string;
  country: string;
  category: string;
  subcategory: string;
  current_rank: number | null;
  previous_rank: number | null;
  delta: number | null;
  status: string; // IN TOP, MOVER UP, MOVER DOWN, ...
};

type WeeklyRow = {
  status: "NEW" | "RE-ENTRY" | "DROPPED";
  rank: number | null;
  app_id: string;
  app_name: string;
  developer_name: string;
  bundle_id: string;
  category: string;
  subcategory: string;
};

type WeeklyPayload = {
  week_start: string | null;
  week_end: string | null;
  counts: Record<string, number>;
  rows: WeeklyRow[];
};

export default function App() {
  const [tab, setTab] = useState<"compare" | "weekly">("compare");

  // meta
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [latestSnapshot, setLatestSnapshot] = useState<string | null>(null);

  // global filters
  const [country, setCountry] = useState("US");
  const [category, setCategory] = useState("all");
  const [subcategory, setSubcategory] = useState("all");
  const [weeklyStatus, setWeeklyStatus] = useState<"" | "NEW" | "RE-ENTRY" | "DROPPED">(""); // вграден в Global filters

  // data
  const [compareRows, setCompareRows] = useState<CompareRow[]>([]);
  const [weekly, setWeekly] = useState<WeeklyPayload>({ week_start: null, week_end: null, counts: {}, rows: [] });

  // sorting
  const [compareSort, setCompareSort] = useState<{ key: keyof CompareRow; dir: "asc" | "desc" } | null>(null);
  const [weeklySort, setWeeklySort] = useState<{ key: keyof WeeklyRow; dir: "asc" | "desc" } | null>({
    key: "rank",
    dir: "asc",
  });

  // pagination
  const PER_PAGE = 50;
  const [pageCompare, setPageCompare] = useState(1);
  const [pageWeekly, setPageWeekly] = useState(1);

  // meta
  async function loadMeta(selCat?: string) {
    const res = await fetch(`${API}/meta${selCat && selCat !== "all" ? `?category=${encodeURIComponent(selCat)}` : ""}`);
    const m = await res.json();
    setCountries(m.countries ?? []);
    setCategories(m.categories ?? []);
    setSubcategories(m.subcategories ?? []);
    if (m.latest_snapshot) setLatestSnapshot(m.latest_snapshot);
  }
  useEffect(() => { loadMeta().catch(() => {}); }, []);
  useEffect(() => { loadMeta(category).catch(() => {}); setSubcategory("all"); }, [category]);

  // loaders
  async function loadCompare() {
    const qs = new URLSearchParams();
    qs.set("limit", "500");
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    const r = await fetch(`${API}/compare?${qs.toString()}`);
    const j = await r.json();
    setCompareRows(j.results ?? []);
    if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
    setPageCompare(1);
  }
  async function loadWeekly() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    if (weeklyStatus) qs.set("status", weeklyStatus);
    const r = await fetch(`${API}/weekly/insights?${qs.toString()}`);
    const j = (await r.json()) as WeeklyPayload;
    setWeekly(j);
    setPageWeekly(1);
  }

  useEffect(() => {
    if (tab === "compare") loadCompare();
    else loadWeekly();
  }, [tab, country, category, subcategory, weeklyStatus]);

  // export
  function exportCompareCSV() {
    const qs = new URLSearchParams();
    qs.set("limit", "500");
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    qs.set("format", "csv");
    window.open(`${API}/compare?${qs.toString()}`, "_blank");
  }
  function exportWeeklyCSV() {
    const qs = new URLSearchParams();
    qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);
    if (weeklyStatus) qs.set("status", weeklyStatus);
    qs.set("format", "csv");
    window.open(`${API}/weekly/insights?${qs.toString()}`, "_blank");
  }
  async function refreshDB() {
    try {
      const r = await fetch(`${API}/admin/refresh`);
      const j = await r.json();
      if (j.latest_snapshot) setLatestSnapshot(j.latest_snapshot);
      if (tab === "compare") loadCompare(); else loadWeekly();
    } catch {}
  }
  function resetFilters() {
    setCountry(countries[0] || "US");
    setCategory("all");
    setSubcategory("all");
    setWeeklyStatus("");
  }

  // sorting
  function sortCompareBy(key: keyof CompareRow) {
    setCompareSort((prev) => ({ key, dir: prev?.key === key && prev.dir === "asc" ? "desc" : "asc" }));
  }
  const compareRowsSorted = useMemo(() => {
    const arr = [...compareRows];
    if (compareSort) {
      const { key, dir } = compareSort;
      arr.sort((a: any, b: any) => {
        const va = a[key] ?? ""; const vb = b[key] ?? "";
        if (typeof va === "number" && typeof vb === "number") return dir === "asc" ? va - vb : vb - va;
        return dir === "asc" ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
      });
    }
    return arr;
  }, [compareRows, compareSort]);

  function sortWeeklyBy(key: keyof WeeklyRow) {
    setWeeklySort((prev) => ({ key, dir: prev?.key === key && prev.dir === "asc" ? "desc" : "asc" }));
  }
  const weeklyRowsSorted = useMemo(() => {
    const arr = [...(weekly.rows || [])];
    if (weeklySort) {
      const { key, dir } = weeklySort;
      arr.sort((a: any, b: any) => {
        const va = a[key] ?? ""; const vb = b[key] ?? "";
        if (typeof va === "number" && typeof vb === "number") return dir === "asc" ? va - vb : vb - va;
        return dir === "asc" ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
      });
    }
    return arr;
  }, [weekly.rows, weeklySort]);

  // pagination slices
  const comparePaged = useMemo(() => {
    const start = (pageCompare - 1) * 50;
    return compareRowsSorted.slice(start, start + 50);
  }, [compareRowsSorted, pageCompare]);
  const weeklyPaged = useMemo(() => {
    const start = (pageWeekly - 1) * 50;
    return weeklyRowsSorted.slice(start, start + 50);
  }, [weeklyRowsSorted, pageWeekly]);

  // status icon for Compare
  function StatusCell(r: CompareRow) {
    const s = (r.status || "").toUpperCase();
    if (s.includes("MOVER UP"))
      return <span style={{ color: "#16a34a", fontWeight: 700 }}>▲ {r.delta ?? ""} MOVER UP</span>;
    if (s.includes("MOVER DOWN"))
      return <span style={{ color: "#dc2626", fontWeight: 700 }}>▼ {Math.abs(r.delta ?? 0)} MOVER DOWN</span>;
    if (s.includes("NEW"))
      return <span style={{ ...pill("#007AFF") }}>NEW</span>;
    if (s.includes("DROPPED"))
      return <span style={{ ...pill("#dc2626") }}>DROPPED</span>;
    return <span style={{ color: "#475467" }}>— IN TOP</span>;
  }

  return (
    <div style={{ fontFamily: "Inter, system-ui, -apple-system", background: "#f4f6fb", minHeight: "100vh" }}>
      <div style={{ maxWidth: 1280, margin: "0 auto", padding: 16 }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
          <img src="/logo.svg" width={28} height={28} alt="Logo" />
          <h1 style={{ margin: 0 }}>App Store Dashboard</h1>
        </div>

        {/* Tabs */}
        <div style={{ display: "flex", gap: 10, marginBottom: 14 }}>
          <button style={btn(tab === "compare")} onClick={() => setTab("compare")}>Compare View</button>
          <button style={btn(tab === "weekly")} onClick={() => setTab("weekly")}>Weekly View</button>
        </div>

        {/* Global filters (Compare + Weekly) */}
        <div style={{ ...box, marginBottom: 16 }}>
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Global filters (Compare)</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
            <select value={country} onChange={(e) => setCountry(e.target.value)} style={{ padding: 6, borderRadius: 8 }}>
              {countries.map((c) => <option key={c}>{c}</option>)}
            </select>
            <select value={category} onChange={(e) => setCategory(e.target.value)} style={{ padding: 6, borderRadius: 8 }}>
              <option value="all">All categories</option>
              {categories.map((c) => <option key={c}>{c}</option>)}
            </select>
            <select value={subcategory} onChange={(e) => setSubcategory(e.target.value)} style={{ padding: 6, borderRadius: 8 }}>
              <option value="all">All subcategories</option>
              {subcategories.map((s) => <option key={s}>{s}</option>)}
            </select>

            {/* статус-филтър, видим винаги, но се използва само в Weekly */}
            <select
              value={weeklyStatus}
              onChange={(e) => setWeeklyStatus(e.target.value as any)}
              title="Weekly status filter"
              style={{ padding: 6, borderRadius: 8 }}
            >
              <option value="">All statuses</option>
              <option value="NEW">NEW</option>
              <option value="RE-ENTRY">RE-ENTRY</option>
              <option value="DROPPED">DROPPED</option>
            </select>

            <button style={btnLite} onClick={resetFilters}>Reset</button>
            <button style={btnLite} onClick={() => (tab === "compare" ? loadCompare() : loadWeekly())}>Reload</button>
            <button style={{ ...btnLite, display: "inline-flex", alignItems: "center", gap: 6 }} onClick={refreshDB}>
              <span>🗂️</span> Refresh DB
            </button>

            {tab === "compare" ? (
              <button style={btnGreen} onClick={exportCompareCSV}>Export CSV</button>
            ) : (
              <button style={btnGreen} onClick={exportWeeklyCSV}>Export CSV</button>
            )}
          </div>
        </div>

        {/* Last updated */}
        <div style={{ marginBottom: 8, color: "#475467" }}>
          Data last updated: <b>{latestSnapshot ?? "N/A"}</b>
        </div>

        {/* ==================== COMPARE VIEW ==================== */}
        {tab === "compare" && (
          <div style={{ ...box }}>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead style={{ background: "#f0f4ff" }}>
                  <tr>
                    {[
                      ["app_name", "App"],
                      ["app_id", "App ID"],
                      ["country", "Country"],
                      ["category", "Category"],
                      ["subcategory", "Subcategory"],
                      ["current_rank", "Current"],
                      ["previous_rank", "Prev"],
                      ["delta", "Δ"],
                      ["status", "Status"],
                    ].map(([key, label]) => (
                      <th
                        key={key}
                        onClick={() => setCompareSort((prev) => ({ key: key as keyof CompareRow, dir: prev?.key === key && prev.dir === "asc" ? "desc" : "asc" }))}
                        style={{
                          textAlign: ["current_rank","previous_rank","delta"].includes(key) ? "right" : "left",
                          padding: 10, cursor: "pointer", whiteSpace: "nowrap",
                        }}
                      >
                        {label} {compareSort?.key === key ? (compareSort.dir === "asc" ? "↑" : "↓") : "↕"}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {comparePaged.length === 0 && (
                    <tr><td colSpan={9} style={{ padding: 12, textAlign: "center", color: "#667085" }}>No data.</td></tr>
                  )}
                  {comparePaged.map((r, i) => (
                    <tr key={r.app_id + i} style={{ borderTop: "1px solid #eef2f7" }}>
                      <td style={{ padding: 10 }}>{r.app_name}</td>
                      <td style={{ padding: 10, whiteSpace: "nowrap" }}>{r.app_id}</td>
                      <td style={{ padding: 10 }}>{r.country}</td>
                      <td style={{ padding: 10 }}>{r.category}</td>
                      <td style={{ padding: 10 }}>{r.subcategory ?? "—"}</td>
                      <td style={{ padding: 10, textAlign: "right" }}>{r.current_rank ?? "—"}</td>
                      <td style={{ padding: 10, textAlign: "right" }}>{r.previous_rank ?? "—"}</td>
                      <td style={{ padding: 10, textAlign: "right", color: (r.delta ?? 0) > 0 ? "#16a34a" : (r.delta ?? 0) < 0 ? "#dc2626" : "#111" }}>
                        {r.delta ?? "—"}
                      </td>
                      <td style={{ padding: 10 }}><StatusCell {...r} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 10 }}>
              <button style={btnLite} onClick={() => setPageCompare((p) => Math.max(1, p - 1))} disabled={pageCompare === 1}>Prev</button>
              <span>Page {pageCompare}</span>
              <button
                style={btnLite}
                onClick={() => setPageCompare((p) => Math.min(Math.ceil(compareRowsSorted.length / 50) || 1, p + 1))}
                disabled={pageCompare * 50 >= compareRowsSorted.length}
              >
                Next
              </button>
            </div>
          </div>
        )}

        {/* ==================== WEEKLY VIEW ==================== */}
        {tab === "weekly" && (
          <div style={{ ...box }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
              <h2 style={{ margin: 0 }}>{formatWeekLabel(weekly.week_start, weekly.week_end)}</h2>
              <div style={{ display: "flex", gap: 10 }}>
                <span style={pill("#EEF2FF", "#1f2937")}>ALL: {weekly.counts?.ALL ?? weekly.rows.length}</span>
                <span style={pill("#007AFF")}>NEW: {weekly.counts?.NEW ?? 0}</span>
                <span style={pill("#16a34a")}>RE-ENTRY: {weekly.counts?.["RE-ENTRY"] ?? 0}</span>
                <span style={pill("#dc2626")}>DROPPED: {weekly.counts?.DROPPED ?? 0}</span>
              </div>
            </div>

            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead style={{ background: "#f0f4ff" }}>
                  <tr>
                    {[
                      ["status","Status"],
                      ["rank","Rank"],
                      ["app_name","App"],
                      ["app_id","App ID"],
                      ["developer_name","Developer"],
                      ["bundle_id","Bundle ID"],
                      ["category","Category"],
                      ["subcategory","Subcategory"],
                    ].map(([key, label]) => (
                      <th
                        key={key}
                        onClick={() => sortWeeklyBy(key as keyof WeeklyRow)}
                        style={{ textAlign: key === "rank" ? "right" : "left", padding: 10, cursor: "pointer", whiteSpace: "nowrap" }}
                      >
                        {label} {weeklySort?.key === key ? (weeklySort.dir === "asc" ? "↑" : "↓") : "↕"}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {weeklyPaged.length === 0 && (
                    <tr><td colSpan={8} style={{ padding: 12, textAlign: "center", color: "#667085" }}>No data.</td></tr>
                  )}
                  {weeklyPaged.map((r, i) => (
                    <tr key={r.app_id + i} style={{ borderTop: "1px solid #eef2f7" }}>
                      <td style={{ padding: 10 }}>
                        {r.status === "NEW" && <span style={pill("#007AFF")}>NEW</span>}
                        {r.status === "RE-ENTRY" && <span style={pill("#16a34a")}>RE-ENTRY</span>}
                        {r.status === "DROPPED" && <span style={pill("#dc2626")}>DROPPED</span>}
                      </td>
                      <td style={{ padding: 10, textAlign: "right" }}>{r.rank ?? "—"}</td>
                      <td style={{ padding: 10 }}>{r.app_name}</td>
                      <td style={{ padding: 10, whiteSpace: "nowrap" }}>{r.app_id}</td>
                      <td style={{ padding: 10 }}>{r.developer_name || "—"}</td>
                      <td style={{ padding: 10, whiteSpace: "nowrap" }}>{r.bundle_id || "—"}</td>
                      <td style={{ padding: 10 }}>{r.category || "—"}</td>
                      <td style={{ padding: 10 }}>{r.subcategory || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 10 }}>
              <button style={btnLite} onClick={() => setPageWeekly((p) => Math.max(1, p - 1))} disabled={pageWeekly === 1}>Prev</button>
              <span>Page {pageWeekly}</span>
              <button
                style={btnLite}
                onClick={() => setPageWeekly((p) => Math.min(Math.ceil((weeklyRowsSorted.length || 0)/50) || 1, p + 1))}
                disabled={pageWeekly * 50 >= (weeklyRowsSorted.length || 0)}
              >Next</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
