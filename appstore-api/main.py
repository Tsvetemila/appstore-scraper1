# appstore-api/main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from pathlib import Path
import os, sqlite3, json, io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime, timedelta

# --- 1️⃣ Сваляне на базата от Google Drive ------------------------------------
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


# --- 2️⃣ Създаване на таблици при нужда --------------------------------------
def ensure_tables_exist(db_path: str):
    print("🧩 Checking database structure...")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    tables = {
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
                snapshot_date TEXT,
                country TEXT,
                app_id TEXT,
                app_name TEXT,
                developer TEXT,
                category TEXT,
                subcategory TEXT,
                rank_now INTEGER,
                rank_prev INTEGER,
                rank_change INTEGER,
                status TEXT
            );
        """
    }
    for name, ddl in tables.items():
        cur.execute(ddl)
    con.commit()
    con.close()
    print("✅ Database structure verified.")


# --- 3️⃣ Попълване на производни таблици при първо стартиране -----------------
def populate_derived_tables(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # --- Проверка дали има колона developer в charts
    cur.execute("PRAGMA table_info(charts);")
    columns = [c[1] for c in cur.fetchall()]
    if "developer" not in columns:
        print("⚙️ Adding missing column 'developer' to charts...")
        cur.execute("ALTER TABLE charts ADD COLUMN developer TEXT;")
        conn.commit()

    # Проверка за данни
    cur.execute("SELECT COUNT(*) FROM charts")
    chart_count = cur.fetchone()[0]
    if chart_count == 0:
        print("⚠️ Charts table empty — nothing to populate.")
        conn.close()
        return

    # --- Попълване на apps
    cur.execute("SELECT COUNT(*) FROM apps")
    apps_count = cur.fetchone()[0]
    if apps_count == 0:
        print("🧩 Populating apps table...")
        try:
            cur.execute("""
                INSERT INTO apps (app_id, name, developer)
                SELECT DISTINCT app_id, app_name, COALESCE(developer, '')
                FROM charts
                WHERE app_id IS NOT NULL;
            """)
            conn.commit()
        except Exception as e:
            print(f"⚠️ Skipping apps population: {e}")

    # --- Попълване на snapshots
    cur.execute("SELECT COUNT(*) FROM snapshots")
    snap_count = cur.fetchone()[0]
    if snap_count == 0:
        print("🧩 Populating snapshots table...")
        cur.execute("""
            INSERT INTO snapshots (snapshot_date, country, category, subcategory, data)
            SELECT DISTINCT snapshot_date, country, category, subcategory, ''
            FROM charts;
        """)
        conn.commit()

    conn.close()
    print("✅ Derived tables populated.")


# --- 4️⃣ Инициализация --------------------------------------------------------
ensure_database_from_drive()

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "appstore-api" / "data" / "app_data.db"
if not DB_PATH.exists():
    DB_PATH = APP_DIR / "data" / "app_data.db"
if not DB_PATH.exists():
    import glob
    matches = glob.glob("**/app_data.db", recursive=True)
    if matches:
        DB_PATH = Path(matches[0]).resolve()
print(f"📘 Using DB: {DB_PATH}")

ensure_tables_exist(str(DB_PATH))
populate_derived_tables(str(DB_PATH))

# --- 5️⃣ FastAPI setup --------------------------------------------------------
app = FastAPI(title="AppStore Charts API", version="1.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://appstore-scraper1.vercel.app",
        "http://localhost:5173",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

def connect():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


# --- 6️⃣ Основни endpoint-и ---------------------------------------------------
@app.get("/meta")
def get_meta():
    size = os.path.getsize(DB_PATH) / (1024 * 1024) if DB_PATH.exists() else 0
    return {"status": "ok", "db_path": str(DB_PATH), "db_size_mb": round(size, 2)}

@app.get("/debug/db-tables")
def debug_db_tables():
    con = connect(); cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    out = {"tables": tables}
    for t in ("apps", "charts", "snapshots", "compare_results"):
        if t in tables:
            cur.execute(f"SELECT COUNT(1) FROM {t}")
            out[f"{t}_rows"] = cur.fetchone()[0]
    con.close()
    return out


@app.get("/charts")
def charts(country: str = "US", limit: int = 50):
    """Връща последния топ 50 за дадена държава"""
    con = connect(); cur = con.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM charts WHERE country=? AND chart_type='top_free'", (country,))
    latest = cur.fetchone()[0]
    if not latest: return {"rows": []}
    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts
        WHERE country=? AND chart_type='top_free' AND snapshot_date=?
        ORDER BY rank ASC LIMIT ?
    """, (country, latest, limit))
    rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    con.close()
    return {"snapshot_date": latest, "rows": rows}


# --- 7️⃣ Weekly comparison logic ---------------------------------------------
@app.get("/compare/weekly-full")
def compare_weekly_full(country: str = "US", lookback_days: int = 7):
    """
    Сравнява последния снапшот (Top 50 Free) с този отпреди lookback_days дни.
    Връща NEW / DROPPED / UP / DOWN / SAME с всички колони.
    """
    con = connect(); cur = con.cursor()

    cur.execute("""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE country=? AND chart_type='top_free'
        ORDER BY snapshot_date DESC LIMIT ?
    """, (country, lookback_days + 1))
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        con.close()
        return {"message": "Not enough snapshots", "results": []}

    latest, previous = dates[0], dates[-1]

    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts WHERE country=? AND chart_type='top_free' AND snapshot_date=?
    """, (country, latest))
    latest_data = {r[0]: r for r in cur.fetchall()}

    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts WHERE country=? AND chart_type='top_free' AND snapshot_date=?
    """, (country, previous))
    prev_data = {r[0]: r for r in cur.fetchall()}

    results = []
    for app_id, row in latest_data.items():
        app_name, dev, cat, subcat, rank_now = row[1], row[2], row[3], row[4], row[5]
        if app_id not in prev_data:
            status, rank_prev, rank_change = "NEW", None, None
        else:
            rank_prev = prev_data[app_id][5]
            rank_change = rank_prev - rank_now
            if rank_change > 0:
                status = "UP"
            elif rank_change < 0:
                status = "DOWN"
            else:
                status = "SAME"
        results.append({
            "app_id": app_id,
            "app_name": app_name,
            "developer": dev,
            "category": cat,
            "subcategory": subcat,
            "rank_now": rank_now,
            "rank_prev": rank_prev,
            "rank_change": rank_change,
            "status": status
        })

    for app_id, row in prev_data.items():
        if app_id not in latest_data:
            app_name, dev, cat, subcat, rank_prev = row[1], row[2], row[3], row[4], row[5]
            results.append({
                "app_id": app_id,
                "app_name": app_name,
                "developer": dev,
                "category": cat,
                "subcategory": subcat,
                "rank_now": None,
                "rank_prev": rank_prev,
                "rank_change": None,
                "status": "DROPPED"
            })

    con.close()
    return {
        "latest_snapshot": latest,
        "previous_snapshot": previous,
        "total_results": len(results),
        "results": results
    }
