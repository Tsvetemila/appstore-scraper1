import React, { useEffect, useMemo, useState } from "react";

type TrendingRow = {
  app_id: string;
  app_name: string;
  min_rank: number | null;
  max_rank: number | null;
  change: number | null;
  country?: string | null;
  chart_type?: string | null;
  category?: string | null;
  subcategory?: string | null;
};

type Compare7 = {
  latest_date: string;
  dates: string[];
  results: {
    app_id: string;
    app_name: string;
    history: { date: string; rank: number | null }[];
  }[];
};

type HistoryRow = {
  snapshot_date: string;
  app_id: string;
  app_name: string;
  rank: number | null;
  country: string | null;
  chart_type: string | null;
  category: string | null;
  subcategory: string | null;
};

const API = "http://127.0.0.1:8000";

const th: React.CSSProperties = { textAlign: "left", padding: "8px", borderBottom: "1px solid #ddd", whiteSpace: "nowrap" };
const td: React.CSSProperties = { padding: "8px", borderBottom: "1px solid #eee" };
const zebra = (i: number): React.CSSProperties => ({ background: i % 2 ? "#fafafa" : "#fff" });

function ArrowChange({ value }: { value: number | null }) {
  if (value === null || value === undefined) return <span>—</span>;
  if (value < 0) return <span title={`${value}`}>▲ {Math.abs(value)}</span>;
  if (value > 0) return <span title={`${value}`}>▼ {value}</span>;
  return <span title="0">→ 0</span>;
}

export default function App() {
  const [tab, setTab] = useState<"trending" | "compare7" | "history">("trending");

  // meta (динамични филтри)
  const [countries, setCountries] = useState<string[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);

  // селекции
  const [country, setCountry] = useState<string>("all");
  const [category, setCategory] = useState<string>("all");
  const [subcategory, setSubcategory] = useState<string>("all");

  // данни
  const [trending, setTrending] = useState<TrendingRow[]>([]);
  const [compare, setCompare] = useState<Compare7>({ latest_date: "", dates: [], results: [] });
  const [history, setHistory] = useState<HistoryRow[]>([]);
  const [historyId, setHistoryId] = useState("");

  // meta – веднъж
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API}/meta`);
        const m = await res.json();
        setCountries(m.countries ?? []);
        setCategories(m.categories ?? []);
        setSubcategories(m.subcategories ?? []);
      } catch (e) {
        console.error("meta error", e);
      }
    })();
  }, []);

  // trending – първоначално (Top 50 Free, без филтри)
  useEffect(() => {
    if (tab !== "trending") return;
    applyTrending(); // по подразбиране: all/all/all
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  async function applyTrending() {
    const qs = new URLSearchParams();
    qs.set("limit", "50");
    qs.set("days", "7");
    if (country !== "all") qs.set("country", country);
    if (category !== "all") qs.set("category", category);
    if (subcategory !== "all") qs.set("subcategory", subcategory);

    const url = `${API}/trending?${qs.toString()}`;
    const r = await fetch(url);
    const j = await r.json();
    setTrending(j.results ?? []);
  }

  // compare – зареждане
  useEffect(() => {
    if (tab !== "compare7") return;
    (async () => {
      const r = await fetch(`${API}/compare7`);
      const j = await r.json();
      setCompare(j);
    })();
  }, [tab]);

  // history – по бутон
  async function loadHistory() {
    if (!historyId.trim()) return;
    const r = await fetch(`${API}/history?app_id=${encodeURIComponent(historyId.trim())}`);
    const j = await r.json();
    setHistory(j.results ?? []);
  }

  const activeBtn = (name: typeof tab): React.CSSProperties =>
    tab === name ? { background: "#1a73e8", color: "#fff", border: "1px solid #1a73e8", marginRight: 8 } : { marginRight: 8 };

  return (
    <div style={{ fontFamily: "Inter, Arial, sans-serif", padding: 20, maxWidth: 1400, margin: "0 auto" }}>
      <h1 style={{ marginTop: 0 }}>📊 App Store Dashboard</h1>

      {/* Навигация */}
      <div style={{ marginBottom: 16 }}>
        <button style={activeBtn("trending")} onClick={() => setTab("trending")}>Trending</button>
        <button style={activeBtn("compare7")} onClick={() => setTab("compare7")}>Сравнение 7 дни</button>
        <button style={activeBtn("history")} onClick={() => setTab("history")}>История (по App ID)</button>
        <button onClick={() => exportCSV(tab, trending, compare, history)} style={{ marginLeft: 8 }}>Export CSV</button>
      </div>

      {/* TRENDING */}
      {tab === "trending" && (
        <>
          <h2 style={{ marginTop: 0 }}>🔥 Trending Apps (Top 50 Free)</h2>

          {/* Филтри */}
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12 }}>
            <select value={country} onChange={(e) => setCountry(e.target.value)}>
              <option value="all">All countries</option>
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

            <button onClick={applyTrending}>Приложи</button>
            <button
              onClick={() => {
                setCountry("all");
                setCategory("all");
                setSubcategory("all");
                setTimeout(applyTrending, 0);
              }}
              title="Изчисти филтрите"
            >
              Reset
            </button>
          </div>

          {/* Таблица */}
          <div style={{ overflowX: "auto", border: "1px solid #eee", borderRadius: 6 }}>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <thead style={{ background: "#f6f8fa" }}>
                <tr>
                  <th style={th}>App</th>
                  <th style={th}>Min rank</th>
                  <th style={th}>Max rank</th>
                  <th style={th}>Change</th>
                  <th style={th}>Country</th>
                  <th style={th}>Category</th>
                  <th style={th}>Subcategory</th>
                </tr>
              </thead>
              <tbody>
                {trending.map((row, i) => (
                  <tr key={row.app_id} style={zebra(i)}>
                    <td style={td}>{row.app_name}</td>
                    <td style={td}>{row.min_rank ?? "—"}</td>
                    <td style={td}>{row.max_rank ?? "—"}</td>
                    <td style={td}><ArrowChange value={row.change ?? null} /></td>
                    <td style={td}>{row.country ?? "—"}</td>
                    <td style={td}>{row.category ?? "—"}</td>
                    <td style={td}>{row.subcategory ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* COMPARE 7 */}
      {tab === "compare7" && (
        <>
          <h2>📅 7-дневно сравнение</h2>
          <div style={{ overflowX: "auto", border: "1px solid #eee", borderRadius: 6 }}>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <thead style={{ background: "#f6f8fa" }}>
                <tr>
                  <th style={th}>App</th>
                  {compare.dates.map((d) => (
                    <th key={d} style={th}>{d}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {compare.results.map((r, i) => (
                  <tr key={r.app_id} style={zebra(i)}>
                    <td style={td}>{r.app_name}</td>
                    {compare.dates.map((d) => (
                      <td key={d} style={td}>
                        {r.history.find((h) => h.date === d)?.rank ?? "—"}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* HISTORY by App ID */}
      {tab === "history" && (
        <>
          <h2>📖 История по App ID (Top Free)</h2>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12 }}>
            <input
              placeholder="Въведи App ID"
              value={historyId}
              onChange={(e) => setHistoryId(e.target.value)}
              style={{ width: 220 }}
            />
            <button onClick={loadHistory}>Зареди</button>
          </div>

          <div style={{ overflowX: "auto", border: "1px solid #eee", borderRadius: 6 }}>
            <table style={{ borderCollapse: "collapse", width: "100%" }}>
              <thead style={{ background: "#f6f8fa" }}>
                <tr>
                  <th style={th}>Дата</th>
                  <th style={th}>App</th>
                  <th style={th}>Rank</th>
                  <th style={th}>Country</th>
                  <th style={th}>Category</th>
                  <th style={th}>Subcategory</th>
                </tr>
              </thead>
              <tbody>
                {history.map((h, i) => (
                  <tr key={`${h.snapshot_date}-${i}`} style={zebra(i)}>
                    <td style={td}>{h.snapshot_date}</td>
                    <td style={td}>{h.app_name}</td>
                    <td style={td}>{h.rank ?? "—"}</td>
                    <td style={td}>{h.country ?? "—"}</td>
                    <td style={td}>{h.category ?? "—"}</td>
                    <td style={td}>{h.subcategory ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

// ------- CSV export -------
function exportCSV(
  tab: "trending" | "compare7" | "history",
  trending: TrendingRow[],
  compare: Compare7,
  history: HistoryRow[]
) {
  let lines: string[] = [];
  if (tab === "trending") {
    lines.push(["App", "MinRank", "MaxRank", "Change", "Country", "Category", "Subcategory"].join(","));
    lines.push(
      ...trending.map((r) =>
        [
          safe(r.app_name),
          r.min_rank ?? "",
          r.max_rank ?? "",
          r.change ?? "",
          r.country ?? "",
          r.category ?? "",
          r.subcategory ?? "",
        ].join(",")
      )
    );
  } else if (tab === "compare7") {
    lines.push(["App", ...compare.dates].join(","));
    for (const r of compare.results) {
      const row = [safe(r.app_name), ...compare.dates.map((d) => r.history.find((h) => h.date === d)?.rank ?? "")];
      lines.push(row.join(","));
    }
  } else {
    lines.push(["Date", "App", "Rank", "Country", "Category", "Subcategory"].join(","));
    lines.push(
      ...history.map((h) =>
        [h.snapshot_date, safe(h.app_name), h.rank ?? "", h.country ?? "", h.category ?? "", h.subcategory ?? ""].join(",")
      )
    );
  }
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${tab}.csv`;
  a.click();
}

function safe(s: string) {
  // за CSV – ограждаме ако има запетая
  return /,|"|\n/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
