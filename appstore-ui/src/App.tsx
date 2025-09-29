import React, { useState } from "react";

type AppData = {
  rank: number | null;
  name: string;
  developer: string;
  delta?: number | null;
  status?: string;
};

export default function App() {
  const [apps, setApps] = useState<AppData[]>([]);
  const [country, setCountry] = useState("US");
  const [category, setCategory] = useState("Overall");
  const [error, setError] = useState<string | null>(null);

  const fetchSnapshot = async () => {
    try {
      await fetch(
        `http://127.0.0.1:8000/fetch?country=${country}&category=${category}`
      );
      alert("✅ Snapshot saved in database!");
    } catch (err) {
      console.error("Fetch error", err);
    }
  };

  const compareSnapshots = async () => {
    try {
      const res = await fetch(
        `http://127.0.0.1:8000/compare?country=${country}&category=${category}`
      );
      const data = await res.json();

      if (data.error) {
        setError("⚠️ Not enough snapshots to compare. Please fetch again.");
        setApps([]);
      } else {
        setError(null);
        setApps(data.apps);
      }
    } catch (err) {
      console.error("Compare error", err);
    }
  };

  const exportCSV = () => {
    if (apps.length === 0) {
      alert("⚠️ No data to export!");
      return;
    }

    const rows = [
      ["Rank", "App", "Developer", "Change"],
      ...apps.map((a) => [
        a.rank ?? "-",
        a.name,
        a.developer,
        a.status === "new"
          ? "🆕 New"
          : a.status === "dropped"
          ? "❌ Dropped"
          : a.delta && a.delta > 0
          ? `🔼 +${a.delta}`
          : a.delta && a.delta < 0
          ? `🔽 ${a.delta}`
          : "➖",
      ]),
    ];
    const csv = rows.map((r) => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "apps.csv";
    a.click();
  };

  const renderDelta = (delta?: number | null, status?: string) => {
    if (status === "new") return <span className="text-blue-500">🆕 New</span>;
    if (status === "dropped") return <span className="text-gray-500">❌ Dropped</span>;
    if (delta === null || delta === undefined)
      return <span className="text-gray-400">➖</span>;
    if (delta > 0) return <span className="text-green-500">🔼 +{delta}</span>;
    if (delta < 0) return <span className="text-red-500">🔽 {delta}</span>;
    return <span className="text-gray-400">➖</span>;
  };

  return (
    <div className="p-6 bg-gray-100 min-h-screen">
      <h1 className="text-3xl font-bold text-center mb-6">
        📱 Top 50 Free Apps ({country})
      </h1>
      <div className="flex justify-center space-x-4 mb-6">
        <select
          value={country}
          onChange={(e) => setCountry(e.target.value)}
          className="border p-2 rounded"
        >
          <option value="US">United States</option>
          <option value="GB">United Kingdom</option>
          <option value="JP">Japan</option>
        </select>
        <select
          value={category}
          onChange={(e) => setCategory(e.target.value)}
          className="border p-2 rounded"
        >
          <option value="Overall">Overall</option>
          <option value="Games">Games</option>
          <option value="Music">Music</option>
        </select>
        <button
          onClick={fetchSnapshot}
          className="bg-green-500 text-white px-4 py-2 rounded"
        >
          Fetch
        </button>
        <button
          onClick={compareSnapshots}
          className="bg-blue-500 text-white px-4 py-2 rounded"
        >
          Compare
        </button>
        <button
          onClick={exportCSV}
          className="bg-gray-700 text-white px-4 py-2 rounded"
        >
          Export
        </button>
      </div>
      {error && <p className="text-center text-red-500 mb-4">{error}</p>}
      <div className="max-w-4xl mx-auto bg-white p-4 rounded shadow">
        <table className="w-full border-collapse">
          <thead>
            <tr className="border-b">
              <th className="p-2 text-left">Rank</th>
              <th className="p-2 text-left">App</th>
              <th className="p-2 text-left">Developer</th>
              <th className="p-2 text-left">Change</th>
            </tr>
          </thead>
          <tbody>
            {apps.map((app, i) => (
              <tr key={i} className="border-b">
                <td className="p-2">{app.rank ?? "-"}</td>
                <td className="p-2">{app.name}</td>
                <td className="p-2">{app.developer}</td>
                <td className="p-2">{renderDelta(app.delta, app.status)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {apps.length === 0 && !error && (
          <p className="text-center text-gray-500 mt-4">No data to show yet.</p>
        )}
      </div>
    </div>
  );
}
