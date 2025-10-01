from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "app_data.db")

app = FastAPI(title="App Store API")

# CORS – фронтът е на Vite (5173)
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

# ---------- META (динамични филтри) ----------
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

    # fallback ако няма стойности
    if not countries:
        countries = ["All countries"]
    if not categories:
        categories = ["All categories"]
    if not subcategories:
        subcategories = ["All subcategories"]

    return {
        "countries": countries,
        "categories": categories,
        "subcategories": subcategories
    }

# ---------- TRENDING: Top 50 FREE, с филтри ----------
class TrendingItem(BaseModel):
    app_id: str
    app_name: str
    min_rank: int | None
    max_rank: int | None
    change: int | None
    country: str
    chart_type: str
    category: str
    subcategory: str

@app.get("/trending")
def get_trending(
    country: str = Query("all"),
    category: str = Query("all"),
    subcategory: str = Query("all"),
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Винаги връщаме Top 50 (limit) за chart='top_free' в последните N дни.
    Филтрите са опционални (all = без ограничение).
    """
    conn = connect()
    cur = conn.cursor()

    latest = get_latest_date(conn)
    if not latest:
        conn.close()
        return {"results": []}

    latest_dt = datetime.fromisoformat(latest)
    start_dt = (latest_dt - timedelta(days=days - 1)).date().isoformat()
    end_dt = latest_dt.date().isoformat()

    # динамично сглобяване на WHERE
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
               (SELECT rank FROM window WHERE app_id = w.app_id ORDER BY snapshot_date ASC  LIMIT 1) AS first_rank,
               (SELECT rank FROM window WHERE app_id = w.app_id ORDER BY snapshot_date DESC LIMIT 1) AS last_rank
        FROM window w
        GROUP BY w.app_id
    )
    SELECT
        w.app_id,
        MAX(w.app_name) AS app_name,
        MIN(w.rank)     AS min_rank,
        MAX(w.rank)     AS max_rank,
        (fl.last_rank - fl.first_rank) AS change,
        (SELECT country     FROM window wx WHERE wx.app_id = w.app_id ORDER BY snapshot_date DESC LIMIT 1) AS country,
        (SELECT chart_type  FROM window wx WHERE wx.app_id = w.app_id ORDER BY snapshot_date DESC LIMIT 1) AS chart_type,
        (SELECT category    FROM window wx WHERE wx.app_id = w.app_id ORDER BY snapshot_date DESC LIMIT 1) AS category,
        (SELECT subcategory FROM window wx WHERE wx.app_id = w.app_id ORDER BY snapshot_date DESC LIMIT 1) AS subcategory
    FROM window w
    JOIN first_last fl ON fl.app_id = w.app_id
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
            "country": r[5] if r[5] else "N/A",
            "chart_type": r[6] if r[6] else "N/A",
            "category": r[7] if r[7] else "N/A",
            "subcategory": r[8] if r[8] else "N/A",
        })
    return {"results": results}
