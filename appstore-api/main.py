# appstore-api/main.py
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from pathlib import Path
import os, sqlite3, json, io, csv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime, timedelta

# -------------------------------- 1) DB Download --------------------------------
def ensure_database_from_drive(force=False):
    local_path = "appstore-api/appstore-api/data/app_data.db"
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if os.path.exists(local_path) and not force:
        print("✅ Database already exists locally.")
        return

    if not creds_json or not folder_id:
        print("⚠️ Missing Google Drive credentials. Skipping DB download.")
        return

    print("⬇️ Downloading latest database from Google Drive...")
    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json))
        drive = build("drive", "v3", credentials=creds)
        results = drive.files().list(
            q=f"'{folder_id}' in parents and name='app_data.db' and trashed=false",
            fields="files(id, name, modifiedTime)"
        ).execute()
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
                if status:
                    print(f"⬇️ Download progress: {int(status.progress() * 100)}%")
        print(f"✅ Database downloaded to {local_path}")
    except Exception as e:
        print(f"❌ Error downloading DB: {e}")


# -------------------------------- 2) Tables -------------------------------------
def ensure_tables_exist(db_path: str):
    print("🧩 Checking database structure...")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
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
    """)
    con.commit()
    con.close()
    print("✅ Database structure verified.")


# -------------------------------- 3) Bootstrap ----------------------------------
ensure_database_from_drive()
APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "appstore-api" / "data" / "app_data.db"
if not DB_PATH.exists():
    DB_PATH = APP_DIR / "data" / "app_data.db"
print(f"📘 Using DB: {DB_PATH}")

ensure_tables_exist(str(DB_PATH))

app = FastAPI(title="AppStore Charts API", version="1.4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://appstore-scraper1.vercel.app",
        "http://localhost:5173",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def connect():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


# -------------------------------- 4) Helpers ------------------------------------
def _where(filters: Dict[str, Optional[str]]) -> (str, List[Any]):
    parts, params = ["chart_type='top_free'"], []
    for col in ("country", "category", "subcategory"):
        val = filters.get(col)
        if val and val != "all":
            parts.append(f"{col}=?")
            params.append(val)
    return " AND ".join(parts), params


# -------------------------------- 5) Meta ---------------------------------------
@app.get("/meta")
def get_meta(category: Optional[str] = None):
    con = connect()
    cur = con.cursor()
    cur.execute("SELECT DISTINCT country FROM charts ORDER BY country")
    countries = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT category FROM charts ORDER BY category")
    categories = [r[0] for r in cur.fetchall()]
    if category and category != "all":
        cur.execute("SELECT DISTINCT subcategory FROM charts WHERE category=? ORDER BY subcategory", (category,))
    else:
        cur.execute("SELECT DISTINCT subcategory FROM charts ORDER BY subcategory")
    subcategories = [r[0] for r in cur.fetchall()]
    con.close()
    return {"countries": countries, "categories": categories, "subcategories": subcategories}


@app.get("/meta/latest-info")
def latest_info(country: str = "US"):
    """Returns last snapshot date for display."""
    con = connect()
    cur = con.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM charts WHERE country=?", (country,))
    latest = cur.fetchone()[0]
    con.close()
    return {"latest_snapshot": latest}


# -------------------------------- 6) Compare ------------------------------------
@app.get("/compare")
def compare(country: str = "US", category: str = None, subcategory: str = None):
    con = connect()
    cur = con.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM charts WHERE country=?", (country,))
    latest = cur.fetchone()[0]
    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts WHERE country=? AND snapshot_date=? ORDER BY rank ASC LIMIT 200
    """, (country, latest))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return {"latest_snapshot": latest, "results": rows}


@app.get("/compare/export")
def export_compare_csv(country: str = "US", category: str = None, subcategory: str = None):
    data = compare(country, category, subcategory)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["app_id", "app_name", "developer", "country", "category", "subcategory", "rank"])
    for r in data["results"]:
        writer.writerow([r["app_id"], r["app_name"], r["developer"], country, r["category"], r["subcategory"], r["rank"]])
    return Response(content=output.getvalue(), media_type="text/csv")


# -------------------------------- 7) Refresh ------------------------------------
@app.get("/refresh-db")
def refresh_db():
    """Force re-download DB from Google Drive."""
    ensure_database_from_drive(force=True)
    return {"status": "Database refreshed from Google Drive."}
