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

        # Кои таблици съществуват в момента?
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
    APP_DIR / "appstore-api" / "data" / "app_data.db",  # ✅ добавен fallback
    APP_DIR.parent / "data" / "app_data.db",            # ✅ fallback към горната папка
    APP_DIR / "data" / "app_data.sqlite",
    APP_DIR / "data" / "app_data.sqlite3",
]


def _resolve_db_path() -> Path:
    for cand in _candidates:
        if cand and cand.exists():
            return cand
    return _candidates[1]


DB_PATH = _resolve_db_path()


def _inspect_database_quick(db_path: str):
    """Лека проверка какво вижда SQLite."""
    try:
        abs_path = os.path.abspath(db_path)
        print(f"🧭 Resolved DB_PATH: {abs_path}")
        print("🔎 DB candidates:", [str(c) for c in _candidates if c is not None])

        if not os.path.exists(db_path):
            print(f"❌ Resolved DB path does not exist on disk: {abs_path}")
            return

        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            tables = [r[0] for r in cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            )]
            print(f"📋 Tables in database: {tables}")

            for t in ("apps", "charts", "snapshots"):
                if t in tables:
                    cur.execute(f"SELECT 1 FROM {t} LIMIT 1;")
                    has_row = cur.fetchone() is not None
                    print(f"   • {t}: {'has data' if has_row else 'empty'}")
    except Exception as e:
        print(f"❌ Error inspecting DB: {e}")


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
_inspect_database_quick(str(DB_PATH))
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


@app.get("/meta")
def get_meta():
    return {"status": "ok", "message": "API connected and DB ready"}


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
