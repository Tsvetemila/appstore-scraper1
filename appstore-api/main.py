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
    local_path = "appstore-api/appstore-api/data/app_data.db"
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

        abs_path = os.path.abspath(local_path)
        if os.path.exists(local_path):
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"📂 Database found at {abs_path} ({size_mb:.2f} MB)")
        else:
            print(f"❌ Database file missing after download! Tried path: {abs_path}")

    except Exception as e:
        print(f"❌ Error downloading database: {e}")


# --- Създаване/осигуряване на таблици в базата -------------------------------
def ensure_tables_exist(db_path: str):
    """Проверява кои таблици ги има и създава липсващите."""
    try:
        print("🧩 Checking database structure...")
        con = sqlite3.connect(db_path)
        cur = con.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing = {r[0] for r in cur.fetchall()}

        required = {
            "apps": """
                CREATE TABLE IF NOT EXISTS apps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_id TEXT,
                    name TEXT,
                    developer TEXT,
                    price TEXT,
                    url TEXT
                );
            """,
            "charts": """
                CREATE TABLE IF NOT EXISTS charts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_date TEXT,
                    country TEXT,
                    chart_type TEXT,
                    category TEXT,
                    subcategory TEXT,
                    rank INTEGER,
                    app_id TEXT,
                    app_name TEXT,
                    developer TEXT
                );
            """,
            "snapshots": """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_date TEXT,
                    country TEXT,
                    category TEXT,
                    subcategory TEXT,
                    data TEXT
                );
            """,
            "compare_results": """
                CREATE TABLE IF NOT EXISTS compare_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country TEXT,
                    category TEXT,
                    subcategory TEXT,
                    new_entries TEXT,
                    dropped_entries TEXT,
                    date_generated TEXT
                );
            """
        }

        for name, ddl in required.items():
            if name not in existing:
                print(f"⚙️ Creating missing table: {name}")
                cur.execute(ddl)

        con.commit()
        con.close()
        print("✅ Database structure verified.")
    except Exception as e:
        print(f"❌ Error ensuring tables: {e}")


# --- Инициализация при стартиране --------------------------------------------
ensure_database_from_drive()

APP_DIR = Path(__file__).resolve().parent
_candidates: List[Optional[Path]] = [
    Path(os.getenv("DB_PATH")) if os.getenv("DB_PATH") else None,
    APP_DIR / "data" / "app_data.db",
    APP_DIR / "appstore-api" / "data" / "app_data.db",
    APP_DIR.parent / "data" / "app_data.db",
    APP_DIR / "data" / "app_data.sqlite",
    APP_DIR / "data" / "app_data.sqlite3",
]


def _resolve_db_path() -> Path:
    for cand in _candidates:
        if cand and cand.exists():
            return cand
    return _candidates[1]


DB_PATH = _resolve_db_path()


# --- Auto-correct for nested directory structures on Render ---
if not DB_PATH.exists():
    import glob
    print(f"⚠️ [DB FIX] DB not found at {DB_PATH}, searching deeper...")
    matches = glob.glob("**/app_data.db", recursive=True)
    if matches:
        new_path = Path(matches[0]).resolve()
        print(f"✅ [DB FIX] Found DB at {new_path}")
        DB_PATH = new_path
    else:
        print("❌ [DB FIX] No DB file found anywhere in project directories.")


# --- Стартиране на проверките -----------------------------------------------
ensure_tables_exist(str(DB_PATH))


# --- FastAPI приложение ------------------------------------------------------
app = FastAPI(title="AppStore Charts API", version="1.1")

_default_origins = {
    "https://appstore-scraper1.vercel.app",
    "https://appstore-scraper1-git-main-tsvetemilias-projects.vercel.app",
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


# --- СЪЩЕСТВУВАЩИ ЕНДПОЙНТИ -------------------------------------------------
@app.get("/meta")
def get_meta():
    size_mb = os.path.getsize(DB_PATH) / (1024 * 1024) if os.path.exists(DB_PATH) else 0
    return {
        "status": "ok",
        "message": "API connected and DB ready",
        "db_path": str(DB_PATH),
        "db_size_mb": round(size_mb, 2)
    }


@app.get("/debug/db-tables")
def debug_db_tables():
    con = connect()
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    out = {"tables": tables}
    for t in ("apps", "charts", "snapshots", "compare_results"):
        if t in tables:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            out[f"{t}_rows"] = cur.fetchone()[0]
    con.close()
    return out


# --- НОВИ РУТОВЕ ЗА ФРОНТЕНДА ------------------------------------------------
from datetime import datetime, timedelta

@app.get("/charts")
def charts(country: str = Query("US"), chart_type: str = Query("top_free"), limit: int = Query(50)):
    """Връща последните данни за избрана държава"""
    con = connect()
    cur = con.cursor()
    cur.execute("""
        SELECT MAX(snapshot_date) FROM charts WHERE country=? AND chart_type=?
    """, (country, chart_type))
    latest_date = cur.fetchone()[0]
    if not latest_date:
        return {"rows": []}

    cur.execute("""
        SELECT app_id, app_name, developer, rank, category, subcategory
        FROM charts
        WHERE country=? AND chart_type=? AND snapshot_date=?
        ORDER BY rank ASC LIMIT ?
    """, (country, chart_type, latest_date, limit))
    rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    con.close()
    return {"snapshot_date": latest_date, "rows": rows}


@app.get("/compare")
def compare(country: str = Query("US"), chart_type: str = Query("top_free"), lookback_days: int = Query(7)):
    """Сравнява последния снапшот спрямо предходен (до 7 дни назад)"""
    con = connect()
    cur = con.cursor()
    cur.execute("""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE country=? AND chart_type=?
        ORDER BY snapshot_date DESC LIMIT 8
    """, (country, chart_type))
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        return {"results": [], "message": "Not enough snapshots to compare."}
    latest, previous = dates[0], dates[1]

    cur.execute("""
        SELECT app_id, rank FROM charts
        WHERE country=? AND chart_type=? AND snapshot_date=?
    """, (country, chart_type, latest))
    latest_ranks = {a: r for a, r in cur.fetchall()}

    cur.execute("""
        SELECT app_id, rank FROM charts
        WHERE country=? AND chart_type=? AND snapshot_date=?
    """, (country, chart_type, previous))
    prev_ranks = {a: r for a, r in cur.fetchall()}

    results = []
    for app_id, rank_now in latest_ranks.items():
        if app_id not in prev_ranks:
            status = "NEW"
            delta = None
        else:
            delta = prev_ranks[app_id] - rank_now
            if delta > 0:
                status = "MOVER_UP"
            elif delta < 0:
                status = "MOVER_DOWN"
            else:
                status = "IN_TOP"
        results.append({"app_id": app_id, "current_rank": rank_now, "delta": delta, "status": status})

    dropouts = [{"app_id": a, "previous_rank": r, "status": "DROPOUT"} for a, r in prev_ranks.items() if a not in latest_ranks]

    con.close()
    return {
        "latest_snapshot": latest,
        "previous_snapshot": previous,
        "results": results,
        "dropouts": dropouts
    }


@app.get("/reports/weekly")
def weekly(country: str = "US", chart_type: str = "top_free"):
    """Връща NEW и DROPOUT за UI седмичния отчет"""
    data = compare(country=country, chart_type=chart_type)
    new_items = [r for r in data["results"] if r["status"] == "NEW"]
    dropouts = data["dropouts"]
    return {
        "latest_snapshot": data["latest_snapshot"],
        "new_entries": new_items,
        "dropouts": dropouts
    }
