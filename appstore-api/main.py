from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import sqlite3
from typing import List, Dict
import os
import pandas as pd

app = FastAPI(title="App Store Charts API")

# Абсолютен път към базата (спрямо папката на файла)
DB_PATH = os.path.join(os.path.dirname(__file__), "app_data.db")

def query_db(query: str, params: tuple = ()) -> List[Dict]:
    """Помощна функция за заявки към SQLite"""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(query, params).fetchall()
    con.close()
    return [dict(row) for row in rows]

@app.get("/")
def root():
    return {"status": "ok", "message": "App Store API working with charts table"}

@app.get("/health")
def health():
    if not os.path.exists(DB_PATH):
        return {"status": "missing", "db_path": DB_PATH, "rows": 0}

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='charts'")
    table = cur.fetchone()
    if not table:
        con.close()
        return {"status": "missing", "db_path": DB_PATH, "rows": 0}

    cur.execute("SELECT COUNT(*) FROM charts")
    count = cur.fetchone()[0]
    con.close()
    return {"status": "ok", "db_path": DB_PATH, "rows": count}

@app.get("/current")
def get_current(country: str = "BG", chart_type: str = "top_free",
                category: str = "Games", subcategory: str = "Arcade"):
    query = """
    SELECT *
    FROM charts
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM charts)
      AND country = ?
      AND chart_type = ?
      AND category = ?
      AND IFNULL(subcategory,'') = ?
    ORDER BY rank ASC
    LIMIT 50
    """
    rows = query_db(query, (country, chart_type, category, subcategory))
    return {"count": len(rows), "results": rows}

@app.get("/available")
def available():
    query = """
    SELECT DISTINCT country, chart_type, category, IFNULL(subcategory,'') AS subcategory
    FROM charts
    ORDER BY country, chart_type, category, subcategory
    """
    rows = query_db(query)
    return rows

@app.get("/compare7")
def compare7(country: str = "BG", chart_type: str = "top_free",
             category: str = "Games", subcategory: str = "Arcade"):
    """
    История на current top50 спрямо всички последни 7 дни.
    За всяко приложение дава ranks ден по ден.
    """
    latest_date_query = "SELECT MAX(snapshot_date) as d FROM charts"
    latest_date = query_db(latest_date_query)[0]["d"]
    if not latest_date:
        raise HTTPException(status_code=404, detail="No data in charts")

    dates_query = """
    SELECT DISTINCT snapshot_date
    FROM charts
    WHERE snapshot_date <= ?
      AND snapshot_date >= date(?, '-6 day')
    ORDER BY snapshot_date ASC
    """
    dates = [r["snapshot_date"] for r in query_db(dates_query, (latest_date, latest_date))]
    if not dates:
        raise HTTPException(status_code=404, detail="No snapshots in last 7 days")

    top_query = """
    SELECT app_id, app_name
    FROM charts
    WHERE snapshot_date = ?
      AND country = ?
      AND chart_type = ?
      AND category = ?
      AND IFNULL(subcategory,'') = ?
    ORDER BY rank ASC
    LIMIT 50
    """
    top_apps = query_db(top_query, (latest_date, country, chart_type, category, subcategory))
    app_ids = [a["app_id"] for a in top_apps]

    if not app_ids:
        raise HTTPException(status_code=404, detail="No apps in current top50")

    hist_query = f"""
    SELECT snapshot_date, app_id, rank
    FROM charts
    WHERE app_id IN ({",".join(["?"]*len(app_ids))})
      AND country = ?
      AND chart_type = ?
      AND category = ?
      AND IFNULL(subcategory,'') = ?
      AND snapshot_date IN ({",".join(["?"]*len(dates))})
    """
    rows = query_db(hist_query, (*app_ids, country, chart_type, category, subcategory, *dates))

    history_map = {a["app_id"]: {"app_name": a["app_name"], "history": {d: None for d in dates}} for a in top_apps}
    for r in rows:
        history_map[r["app_id"]]["history"][r["snapshot_date"]] = r["rank"]

    results = []
    for app_id, data in history_map.items():
        results.append({
            "app_id": app_id,
            "app_name": data["app_name"],
            "history": [{"date": d, "rank": data["history"][d]} for d in dates]
        })

    return {"latest_date": latest_date, "dates": dates, "count": len(results), "results": results}

@app.get("/trending")
def trending(country: str = "BG", chart_type: str = "top_free",
             category: str = "Games", subcategory: str = "Arcade", days: int = 7, limit: int = 10):
    """
    Приложенията с най-голямо движение (up/down) в рамките на последните N дни.
    """
    latest_date_query = "SELECT MAX(snapshot_date) as d FROM charts"
    latest_date = query_db(latest_date_query)[0]["d"]
    if not latest_date:
        raise HTTPException(status_code=404, detail="No data in charts")

    dates_query = """
    SELECT DISTINCT snapshot_date
    FROM charts
    WHERE snapshot_date <= ?
      AND snapshot_date >= date(?, ?)
    ORDER BY snapshot_date ASC
    """
    dates = [r["snapshot_date"] for r in query_db(dates_query, (latest_date, latest_date, f"-{days-1} day"))]
    if not dates:
        raise HTTPException(status_code=404, detail=f"No snapshots in last {days} days")

    hist_query = f"""
    SELECT snapshot_date, app_id, app_name, rank
    FROM charts
    WHERE country = ?
      AND chart_type = ?
      AND category = ?
      AND IFNULL(subcategory,'') = ?
      AND snapshot_date IN ({",".join(["?"]*len(dates))})
    """
    rows = query_db(hist_query, (country, chart_type, category, subcategory, *dates))

    app_map = {}
    for r in rows:
        app_id = r["app_id"]
        if app_id not in app_map:
            app_map[app_id] = {"app_name": r["app_name"], "ranks": []}
        app_map[app_id]["ranks"].append(r["rank"])

    movements = []
    for app_id, data in app_map.items():
        if not data["ranks"]:
            continue
        min_rank = min(data["ranks"])
        max_rank = max(data["ranks"])
        change = max_rank - min_rank
        movements.append({
            "app_id": app_id,
            "app_name": data["app_name"],
            "min_rank": min_rank,
            "max_rank": max_rank,
            "change": change,
            "history_len": len(data["ranks"])
        })

    movements_sorted = sorted(movements, key=lambda x: abs(x["change"]), reverse=True)

    return {
        "latest_date": latest_date,
        "dates": dates,
        "days": days,
        "results": movements_sorted[:limit]
    }

@app.get("/history")
def history(app_id: str, days: int = 7, country: str = "BG",
            chart_type: str = "top_free", category: str = "Games", subcategory: str = "Arcade"):
    query = """
    SELECT snapshot_date, app_id, app_name, rank, country, chart_type, category, IFNULL(subcategory,'') as subcategory
    FROM charts
    WHERE app_id = ?
      AND country = ?
      AND chart_type = ?
      AND category = ?
      AND IFNULL(subcategory,'') = ?
      AND snapshot_date >= date((SELECT MAX(snapshot_date) FROM charts), ?)
    ORDER BY snapshot_date ASC
    """
    days_param = f"-{days} day"
    rows = query_db(query, (app_id, country, chart_type, category, subcategory, days_param))

    if not rows:
        raise HTTPException(status_code=404, detail="No history for this app")

    return {"app_id": app_id, "days": days, "count": len(rows), "results": rows}

@app.get("/export_csv")
def export_csv(country: str = "BG", chart_type: str = "top_free",
               category: str = "Games", subcategory: str = "Arcade"):
    query = """
    SELECT *
    FROM charts
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM charts)
      AND country = ?
      AND chart_type = ?
      AND category = ?
      AND IFNULL(subcategory,'') = ?
    ORDER BY rank ASC
    LIMIT 50
    """
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, con, params=(country, chart_type, category, subcategory))
    con.close()

    if df.empty:
        raise HTTPException(status_code=404, detail="No data for this context")

    out_path = f"export_{country}_{chart_type}_{category}_{subcategory}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    return FileResponse(out_path, media_type="text/csv", filename=os.path.basename(out_path))
