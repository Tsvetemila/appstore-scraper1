from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import sqlite3
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "app_data.db")

app = FastAPI(title="App Store API")

# CORS – за фронтенда
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def connect():
    return sqlite3.connect(DB_PATH)

def get_latest_date(conn: sqlite3.Connection) -> str:
    cur = conn.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM charts")
    row = cur.fetchone()
    return row[0] if row and row[0] else None

# ---------- HEALTH ----------
@app.get("/health")
def health():
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='charts'")
        ok = cur.fetchone() is not None
        conn.close()
        return {"ok": ok, "db": DB_PATH, "db_error": None}
    except Exception as e:
        return {"ok": False, "db": DB_PATH, "db_error": str(e)}

# ---------- META ----------
@app.get("/meta")
def meta():
    conn = connect()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT country FROM charts WHERE country IS NOT NULL AND country <> '' ORDER BY country")
    countries = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT category FROM charts WHERE category IS NOT NULL AND category <> '' ORDER BY category")
    categories = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT subcategory FROM charts WHERE subcategory IS NOT NULL AND subcategory <> '' ORDER BY subcategory")
    subcategories = [r[0] for r in cur.fetchall()]

    conn.close()

    return {
        "countries": countries or ["All countries"],
        "categories": categories or ["All categories"],
        "subcategories": subcategories or ["All subcategories"]
    }

# ---------- TRENDING ----------
@app.get("/trending")
def get_trending(
    country: str = Query("all"),
    category: str = Query("all"),
    subcategory: str = Query("all"),
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200)
):
    conn = connect()
    cur = conn.cursor()

    latest = get_latest_date(conn)
    if not latest:
        conn.close()
        return {"results": []}

    latest_dt = datetime.fromisoformat(latest)
    start_dt = (latest_dt - timedelta(days=days - 1)).date().isoformat()
    end_dt = latest_dt.date().isoformat()

    params = [start_dt, end_dt]
    where_extra = ""
    if country.lower() != "all":
        where_extra += " AND country = ?"
        params.append(country)
    if category.lower() != "all":
        where_extra += " AND category = ?"
        params.append(category)
    if subcategory.lower() != "all":
        where_extra += " AND subcategory = ?"
        params.append(subcategory)

    sql = f"""
    WITH window AS (
        SELECT snapshot_date, app_id, app_name, rank, country, chart_type, category, subcategory
        FROM charts
        WHERE chart_type = 'top_free'
          AND snapshot_date BETWEEN ? AND ?
          {where_extra}
    ),
    first_last AS (
        SELECT w.app_id,
               (SELECT rank FROM window WHERE app_id = w.app_id ORDER BY snapshot_date ASC LIMIT 1),
               (SELECT rank FROM window WHERE app_id = w.app_id ORDER BY snapshot_date DESC LIMIT 1)
        FROM window w
        GROUP BY w.app_id
    )
    SELECT
        w.app_id,
        MAX(w.app_name) AS app_name,
        MIN(w.rank)     AS min_rank,
        MAX(w.rank)     AS max_rank,
        ( (SELECT rank FROM window WHERE app_id=w.app_id ORDER BY snapshot_date DESC LIMIT 1)
        - (SELECT rank FROM window WHERE app_id=w.app_id ORDER BY snapshot_date ASC  LIMIT 1) ) AS change,
        MAX(w.country), MAX(w.chart_type), MAX(w.category), MAX(w.subcategory)
    FROM window w
    GROUP BY w.app_id
    ORDER BY MIN(w.rank) ASC
    LIMIT ?
    """
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    results = []
    for r in rows:
        results.append({
            "app_id": r[0],
            "app_name": r[1],
            "min_rank": r[2],
            "max_rank": r[3],
            "change": r[4] if r[4] is not None else 0,
            "country": r[5] or "N/A",
            "chart_type": r[6] or "N/A",
            "category": r[7] or "N/A",
            "subcategory": r[8] or "—",
        })
    return {"results": results}

# ---------- COMPARE 7 ----------
@app.get("/compare7")
def compare7(
    country: str = Query("all"),
    category: str = Query("all"),
    subcategory: str = Query("all"),
    limit: int = Query(50, ge=1, le=200)
):
    conn = connect()
    cur = conn.cursor()

    latest = get_latest_date(conn)
    if not latest:
        conn.close()
        return {"results": []}

    latest_dt = datetime.fromisoformat(latest).date()
    start_dt = latest_dt - timedelta(days=7)

    params = []
    where_extra = ""
    if country.lower() != "all":
        where_extra += " AND country = ?"
        params.append(country)
    if category.lower() != "all":
        where_extra += " AND category = ?"
        params.append(category)
    if subcategory.lower() != "all":
        where_extra += " AND subcategory = ?"
        params.append(subcategory)

    sql_latest = f"""
      SELECT app_id, app_name, rank, country, category, subcategory
      FROM charts
      WHERE snapshot_date = ? {where_extra}
      ORDER BY rank ASC LIMIT {limit}
    """
    cur.execute(sql_latest, [latest] + params)
    latest_rows = {r[0]: {
        "app_id": r[0], "app_name": r[1], "current_rank": r[2],
        "country": r[3], "category": r[4], "subcategory": r[5]
    } for r in cur.fetchall()}

    sql_prev = f"""
      SELECT app_id, app_name, MIN(rank), MAX(snapshot_date), country, category, subcategory
      FROM charts
      WHERE snapshot_date BETWEEN ? AND ? {where_extra}
      GROUP BY app_id
    """
    cur.execute(sql_prev, [start_dt.isoformat(), (latest_dt - timedelta(days=1)).isoformat()] + params)
    prev_rows = {r[0]: {
        "app_name": r[1],
        "rank": r[2],
        "last_seen": r[3],
        "country": r[4],
        "category": r[5],
        "subcategory": r[6]
    } for r in cur.fetchall()}

    conn.close()

    results = []
    for app_id, data in latest_rows.items():
        prev = prev_rows.get(app_id)
        if prev is None:
            results.append({**data, "previous_rank": None, "delta": None, "status": "NEW"})
        else:
            prev_rank = prev["rank"]
            delta = prev_rank - data["current_rank"]
            status = "MOVER UP" if delta > 0 else "MOVER DOWN" if delta < 0 else "IN_TOP"
            results.append({**data, "previous_rank": prev_rank, "delta": delta, "status": status})

    for app_id, prev in prev_rows.items():
        if app_id not in latest_rows:
            results.append({
                "app_id": app_id,
                "app_name": prev["app_name"],
                "current_rank": None,
                "previous_rank": prev["rank"],
                "delta": None,
                "status": "DROPOUT",
                "country": prev["country"],
                "category": prev["category"],
                "subcategory": prev["subcategory"],
            })

    return {"results": results, "latest_date": latest, "compared_days": 7}

# ---------- EXPORT ----------
@app.get("/export")
def export_csv(
    country: str = Query("all"),
    category: str = Query("all"),
    subcategory: str = Query("all")
):
    data = compare7(country, category, subcategory)["results"]

    def generate():
        header = ["name","category","subcategory","country","current_rank","previous_rank","status","delta","app_id","snapshot_timestamp","snapshot_week","source"]
        yield ",".join(header) + "\n"
        for row in data:
            line = [
                row.get("app_name",""),
                row.get("category",""),
                row.get("subcategory",""),
                row.get("country",""),
                str(row.get("current_rank") or ""),
                str(row.get("previous_rank") or ""),
                row.get("status",""),
                str(row.get("delta") or ""),
                row.get("app_id",""),
                datetime.utcnow().isoformat(),
                datetime.utcnow().strftime("%G-W%V"),
                "apple_rss"
            ]
            yield ",".join(line) + "\n"

    return StreamingResponse(generate(), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=apps_top50.csv"})
