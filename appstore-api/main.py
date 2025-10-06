# appstore-api/main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, List, Dict, Any
from pathlib import Path
import os, sqlite3, csv, subprocess, sys
from io import StringIO

# --- Добавено: автоматично сваляне на базата от Google Drive при стартиране ---
import io, json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

def ensure_database_from_drive():
    local_path = "appstore-api/data/app_data.db"
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if os.path.exists(local_path):
        print("✅ Database already exists locally.")
        return

    if not creds_json or not folder_id:
        print("⚠️ Missing Google Drive credentials. Skipping DB download.")
        return

    print("⬇️ Database not found — downloading latest from Google Drive...")

    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json))
        drive = build("drive", "v3", credentials=creds)
        query = f"'{folder_id}' in parents and name = 'app_data.db' and trashed = false"
        results = drive.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
        files = results.get("files", [])

        if not files:
            print("⚠️ No app_data.db found in Drive folder.")
            return

        file_id = files[0]["id"]
        request = drive.files().get_media(fileId=file_id)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with io.FileIO(local_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"⬇️ Download progress: {int(status.progress() * 100)}%")

        print(f"✅ Database downloaded to {local_path}")

    except Exception as e:
        print(f"❌ Error downloading database: {e}")

# Извиква се веднага при стартиране на приложението (например в Render)
ensure_database_from_drive()

# --- Локализация на пътищата/базата -----------------------------------------
APP_DIR = Path(__file__).resolve().parent

_candidates: List[Optional[Path]] = [
    Path(os.getenv("DB_PATH")) if os.getenv("DB_PATH") else None,
    APP_DIR / "data" / "app_data.db",
    APP_DIR / "data" / "app_data.sqlite",
    APP_DIR / "data" / "app_data.sqlite3",
]

def _resolve_db_path() -> Path:
    for cand in _candidates:
        if cand and cand.exists():
            return cand
    return _candidates[1]

DB_PATH = _resolve_db_path()

# -----------------------------------------------------------------------------
app = FastAPI(title="AppStore Charts API", version="1.1")

# --- CORS --------------------------------------------------------------------
_default_origins = {
    "https://appstore-scraper1.vercel.app",
    "https://appstore-scraper1-git-main-tsvetemilias-projects.vercel.app",  # 👈 добавен Vercel домейн
    "http://localhost:5173",
    "http://127.0.0.1:5173",
}
_env_origins = {o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()}
ALLOWED_ORIGINS = sorted((_default_origins | _env_origins) - {""})

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# --- Помощни функции ----------------------------------------------------------
def connect() -> sqlite3.Connection:
    if not Path(DB_PATH).exists():
        tried = [str(c) for c in _candidates if c is not None]
        raise HTTPException(
            status_code=503,
            detail={"error": "Database file not found", "tried_paths": tried},
        )
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con

def latest_n_snapshot_dates(con: sqlite3.Connection, n: int = 8) -> List[str]:
    rows = con.execute(
        "SELECT DISTINCT snapshot_date FROM charts ORDER BY snapshot_date DESC LIMIT ?",
        (n,),
    ).fetchall()
    return [r["snapshot_date"] for r in rows]

def load_dimension_rows(con: sqlite3.Connection, snapshot_date: str, country: Optional[str],
                        category: Optional[str], subcategory: Optional[str]) -> Dict[str, Dict[str, Any]]:
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

# --- META endpoint ------------------------------------------------------------
@app.get("/meta")
def meta():
    with connect() as con:
        countries = [r["country"] for r in con.execute("SELECT DISTINCT country FROM charts ORDER BY country")]
        categories = [r["category"] for r in con.execute("SELECT DISTINCT category FROM charts ORDER BY category")]
        subcategories = [r["subcategory"] for r in con.execute(
            "SELECT DISTINCT subcategory FROM charts WHERE subcategory IS NOT NULL ORDER BY subcategory"
        )]
    return {"countries": countries, "categories": categories, "subcategories": subcategories}

# --- /compare7 endpoint -------------------------------------------------------
@app.get("/compare7")
def compare7(country: Optional[str] = Query(None),
             category: Optional[str] = Query(None),
             subcategory: Optional[str] = Query(None),
             limit: int = Query(50, ge=1, le=200)):
    if country and country.lower() == "all":
        country = None
    if category and category.lower() == "all":
        category = None
    if subcategory and subcategory.lower() == "all":
        subcategory = None
    subcat_filter = "" if (subcategory is None and category and category != "Games") else subcategory

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
            r["previous_rank"] if r["previous_rank"] is not None else 10**9,
        ))
        if limit:
            results = results[:limit]
        return {"snapshot": snapN, "previous_snapshot": snapPrev, "results": results}

# --- /reports/weekly endpoint -------------------------------------------------
@app.get("/reports/weekly")
def weekly_report(country: Optional[str] = Query(None),
                  category: Optional[str] = Query(None),
                  subcategory: Optional[str] = Query(None),
                  format: str = Query("json", pattern="^(json|csv)$")):
    if country and country.lower() == "all":
        country = None
    if category and category.lower() == "all":
        category = None
    if subcategory and subcategory.lower() == "all":
        subcategory = None
    subcat_filter = "" if (subcategory is None and category and category != "Games") else subcategory

    with connect() as con:
        dates = latest_n_snapshot_dates(con, 8)
        if len(dates) < 2:
            return {"message": "Not enough snapshots."}

        snapN = dates[0]
        older_dates = dates[1:]
        cur_map = load_dimension_rows(con, snapN, country, category, subcat_filter)
        older_maps = [load_dimension_rows(con, d, country, category, subcat_filter) for d in older_dates]

        prev_union: Dict[str, Dict[str, Any]] = {}
        for m in older_maps:
            prev_union.update(m)

        new_apps, dropouts = [], []
        for app_id, cur in cur_map.items():
            seen_before = any(app_id in om for om in older_maps)
            if not seen_before:
                new_apps.append(cur)
        prev_ids = set(prev_union.keys())
        drop_ids = [pid for pid in prev_ids if pid not in cur_map]
        for pid in drop_ids:
            dropouts.append(prev_union[pid])

        result = {"snapshot": snapN, "compared_snapshots": older_dates,
                  "country": country or "All", "category": category or "All",
                  "subcategory": subcategory or "All",
                  "count_new": len(new_apps), "count_dropped": len(dropouts),
                  "new": new_apps, "dropped": dropouts}

        if format == "csv":
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(
                ["status","country","category","subcategory","rank","app_id","app_name","developer_name"])
            for a in new_apps:
                writer.writerow(["NEW", a["country"], a["category"], a["subcategory"],
                                 a["rank"], a["app_id"], a["app_name"], a["developer_name"]])
            for a in dropouts:
                writer.writerow(["DROPOUT", a["country"], a["category"], a["subcategory"],
                                 a["rank"], a["app_id"], a["app_name"], a["developer_name"]])
            output.seek(0)
            headers = {"Content-Disposition": f"attachment; filename=weekly_report_{snapN}.csv"}
            return StreamingResponse(output, media_type="text/csv", headers=headers)
        return JSONResponse(result)

# --- Health -------------------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "message": "AppStore Charts API running"}

# --- SCRAPER trigger ----------------------------------------------------------
@app.get("/run-scraper")
def run_scraper():
    """Ръчно стартира двата скрейпа и merge_results.py."""
    try:
        scripts = ["scraper_apps.py", "scraper_games.py", "merge_results.py"]
        results = []
        for s in scripts:
            print(f"🚀 Running {s} ...")
            proc = subprocess.run([sys.executable, s], cwd=APP_DIR, capture_output=True, text=True)
            results.append({"script": s, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr})
        return {"status": "done", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
