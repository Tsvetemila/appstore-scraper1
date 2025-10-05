from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, List, Dict, Any
import os, sqlite3, csv
from io import StringIO

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "data", "app_data.db")

app = FastAPI(title="AppStore Charts API", version="1.1")

# --- CORS настройка ---
# Разрешени фронтенд домейни (статични) + опционално от ENV: ALLOWED_ORIGINS="https://foo.app,https://bar.com"
_default_origins = {
    "https://appstore-scraper1.vercel.app",   # продукционният фронтенд
    "http://localhost:5173",                   # локална разработка (Vite)
    "http://127.0.0.1:5173",
}
_env_origins = {
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "").split(",")
    if o.strip()
}
ALLOWED_ORIGINS = sorted((_default_origins | _env_origins) - {""})

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,   # важно: конкретни домейни, не "*"
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# --- Помощни функции ---
def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def latest_n_snapshot_dates(con: sqlite3.Connection, n: int = 8) -> List[str]:
    rows = con.execute(
        "SELECT DISTINCT snapshot_date FROM charts ORDER BY snapshot_date DESC LIMIT ?",
        (n,)
    ).fetchall()
    return [r["snapshot_date"] for r in rows]

def load_dimension_rows(
    con: sqlite3.Connection,
    snapshot_date: str,
    country: Optional[str],
    category: Optional[str],
    subcategory: Optional[str]
) -> Dict[str, Dict[str, Any]]:
    where = ["snapshot_date = ?", "chart_type = 'top_free'"]
    params: List[Any] = [snapshot_date]

    if country:
        where.append("country = ?")
        params.append(country.upper())
    if category:
        where.append("category = ?")
        params.append(category)
    if subcategory is not None:
        if subcategory == "":
            where.append("subcategory IS NULL")
        else:
            where.append("subcategory = ?")
            params.append(subcategory)

    sql = f"""
        SELECT country, category, subcategory, rank, app_id, app_name,
               developer_name, bundle_id
        FROM charts
        WHERE {' AND '.join(where)}
    """
    rows = con.execute(sql, params).fetchall()
    return {str(r["app_id"]): dict(r) for r in rows}

def seen_in_older(app_id: str, older_sets: List[Dict[str, Dict[str, Any]]]) -> bool:
    for s in older_sets:
        if app_id in s:
            return True
    return False

# --- META endpoint ---
@app.get("/meta")
def meta():
    with connect() as con:
        countries = [r["country"] for r in con.execute(
            "SELECT DISTINCT country FROM charts ORDER BY country").fetchall()]
        categories = [r["category"] for r in con.execute(
            "SELECT DISTINCT category FROM charts ORDER BY category").fetchall()]
        subcategories = [r["subcategory"] for r in con.execute(
            "SELECT DISTINCT subcategory FROM charts WHERE subcategory IS NOT NULL ORDER BY subcategory").fetchall()]
    return {"countries": countries, "categories": categories, "subcategories": subcategories}

# --- /compare7 endpoint ---
@app.get("/compare7")
def compare7(
    country: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    subcategory: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200)
):
    if country and country.lower() == "all": country = None
    if category and category.lower() == "all": category = None
    if subcategory and subcategory.lower() == "all": subcategory = None

    if (subcategory is None) and (category and category != "Games"):
        subcat_filter = ""
    else:
        subcat_filter = subcategory

    with connect() as con:
        dates = latest_n_snapshot_dates(con, 8)
        if len(dates) < 2:
            return {"results": [], "message": "Not enough snapshots yet."}

        snapN, snapPrev = dates[0], dates[1]
        older_dates = dates[2:]

        cur_map = load_dimension_rows(con, snapN, country, category, subcat_filter)
        prev_map = load_dimension_rows(con, snapPrev, country, category, subcat_filter)
        older_maps = [load_dimension_rows(con, d, country, category, subcat_filter) for d in older_dates]

        all_ids = set(cur_map.keys()) | set(prev_map.keys())
        results = []

        for app_id in all_ids:
            cur = cur_map.get(app_id)
            prev = prev_map.get(app_id)
            row = {
                "app_id": app_id,
                "app_name": (cur or prev or {}).get("app_name"),
                "country": (cur or prev or {}).get("country"),
                "category": (cur or prev or {}).get("category"),
                "subcategory": (cur or prev or {}).get("subcategory"),
                "current_rank": cur["rank"] if cur else None,
                "previous_rank": prev["rank"] if prev else None,
                "delta": None,
                "status": "IN_TOP",
            }

            if cur and prev:
                delta = prev["rank"] - cur["rank"]
                row["delta"] = delta
                row["status"] = "MOVER UP" if delta > 0 else "MOVER DOWN" if delta < 0 else "IN_TOP"
            elif cur and not prev:
                reentry = seen_in_older(app_id, older_maps)
                row["status"] = "RE-ENTRY" if reentry else "NEW"
            elif prev and not cur:
                row["status"] = "DROPOUT"
            results.append(row)

        results.sort(key=lambda r: (
            r["current_rank"] if r["current_rank"] is not None else 10**9,
            r["previous_rank"] if r["previous_rank"] is not None else 10**9
        ))

        if limit:
            results = results[:limit]

        return {"snapshot": snapN, "previous_snapshot": snapPrev, "results": results}

# --- /reports/weekly endpoint ---
@app.get("/reports/weekly")
def weekly_report(
    country: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    subcategory: Optional[str] = Query(None),
    format: str = Query("json", regex="^(json|csv)$"),
):
    """
    Връща NEW и DROPOUT приложения спрямо последните 7 snapshot-а:
      NEW = не е присъствало в никой от N-1..N-7
      DROPOUT = присъствало е в поне един от N-1..N-7, но липсва в N
    """
    if country and country.lower() == "all": country = None
    if category and category.lower() == "all": category = None
    if subcategory and subcategory.lower() == "all": subcategory = None

    subcat_filter = "" if (subcategory is None and category and category != "Games") else subcategory

    with connect() as con:
        dates = latest_n_snapshot_dates(con, 8)
        if len(dates) < 2:
            return {"message": "Not enough snapshots."}

        snapN = dates[0]
        older_dates = dates[1:]  # N-1 … N-7

        cur_map = load_dimension_rows(con, snapN, country, category, subcat_filter)
        older_maps = [load_dimension_rows(con, d, country, category, subcat_filter) for d in older_dates]

        # Обединяваме всички стари snapshot-и
        prev_union = {}
        for m in older_maps:
            prev_union.update(m)

        new_apps, dropouts = [], []

        # NEW: в N, но не в никой от N-1..N-7
        for app_id, cur in cur_map.items():
            seen_before = any(app_id in om for om in older_maps)
            if not seen_before:
                new_apps.append(cur)

        # DROPOUT: бил в N-1..N-7, но липсва в N
        prev_ids = set(prev_union.keys())
        drop_ids = [pid for pid in prev_ids if pid not in cur_map]
        for pid in drop_ids:
            dropouts.append(prev_union[pid])

        result = {
            "snapshot": snapN,
            "compared_snapshots": older_dates,
            "country": country or "All",
            "category": category or "All",
            "subcategory": subcategory or "All",
            "count_new": len(new_apps),
            "count_dropped": len(dropouts),
            "new": new_apps,
            "dropped": dropouts,
        }

        if format == "csv":
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(["status", "country", "category", "subcategory", "rank", "app_id", "app_name", "developer_name"])
            for a in new_apps:
                writer.writerow(["NEW", a["country"], a["category"], a["subcategory"], a["rank"], a["app_id"], a["app_name"], a["developer_name"]])
            for a in dropouts:
                writer.writerow(["DROPOUT", a["country"], a["category"], a["subcategory"], a["rank"], a["app_id"], a["app_name"], a["developer_name"]])
            output.seek(0)
            headers = {"Content-Disposition": f"attachment; filename=weekly_report_{snapN}.csv"}
            return StreamingResponse(output, media_type="text/csv", headers=headers)

        return JSONResponse(result)

# --- Root endpoint ---
@app.get("/")
def root():
    return {"status": "ok", "message": "AppStore Charts API running"}
