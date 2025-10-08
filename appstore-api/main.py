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

    # --- Проверка дали има нужните колони в charts
    cur.execute("PRAGMA table_info(charts);")
    charts_columns = [c[1] for c in cur.fetchall()]
    if "developer" not in charts_columns:
        print("⚙️ Adding missing column 'developer' to charts...")
        cur.execute("ALTER TABLE charts ADD COLUMN developer TEXT;")
        conn.commit()

    # --- Проверка и добавяне на колони в snapshots
    cur.execute("PRAGMA table_info(snapshots);")
    snap_columns = [c[1] for c in cur.fetchall()]
    added = []
    if "category" not in snap_columns:
        cur.execute("ALTER TABLE snapshots ADD COLUMN category TEXT;")
        added.append("category")
    if "subcategory" not in snap_columns:
        cur.execute("ALTER TABLE snapshots ADD COLUMN subcategory TEXT;")
        added.append("subcategory")
    if "data" not in snap_columns:
        cur.execute("ALTER TABLE snapshots ADD COLUMN data TEXT;")
        added.append("data")
    if added:
        conn.commit()
        print(f"⚙️ Added missing columns to snapshots: {', '.join(added)}")

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
        try:
            cur.execute("""
                INSERT INTO snapshots (snapshot_date, country, category, subcategory, data)
                SELECT DISTINCT snapshot_date, country, category, subcategory, ''
                FROM charts;
            """)
            conn.commit()
        except Exception as e:
            print(f"⚠️ Skipping snapshots population: {e}")

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
    """Връща динамични филтри за frontend (страни, категории, подкатегории)."""
    con = connect()
    cur = con.cursor()
    data = {}
    try:
        cur.execute("SELECT DISTINCT country FROM charts WHERE country IS NOT NULL ORDER BY country")
        data["countries"] = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT category FROM charts WHERE category IS NOT NULL ORDER BY category")
        data["categories"] = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT subcategory FROM charts WHERE subcategory IS NOT NULL ORDER BY subcategory")
        data["subcategories"] = [r[0] for r in cur.fetchall()]
    except Exception as e:
        print(f"⚠️ Error in /meta: {e}")
        data = {"countries": [], "categories": [], "subcategories": []}
    finally:
        con.close()
    return data


@app.get("/charts")
def charts(country: str = "US", limit: int = 50):
    """Връща последния топ 50 за дадена държава."""
    con = connect(); cur = con.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM charts WHERE country=? AND chart_type='top_free'", (country,))
    latest = cur.fetchone()[0]
    if not latest:
        con.close()
        return {"rows": []}
    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts
        WHERE country=? AND chart_type='top_free' AND snapshot_date=?
        ORDER BY rank ASC LIMIT ?
    """, (country, latest, limit))
    rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    con.close()
    return {"snapshot_date": latest, "rows": rows}


# --- 7️⃣ Weekly comparison logic (last 7 days) -------------------------------

@app.get("/compare/weekly-full")
def compare_weekly_full(country: str = "US", lookback_days: int = 7):
    """
    Сравнява последния снапшот спрямо всички снапшоти от последните 7 дни.
    Връща NEW / DROPPED / UP / DOWN / SAME.
    """
    con = connect()
    cur = con.cursor()

    # Намираме най-новия snapshot
    cur.execute("""
        SELECT MAX(snapshot_date) FROM charts
        WHERE country=? AND chart_type='top_free'
    """, (country,))
    latest = cur.fetchone()[0]
    if not latest:
        con.close()
        return {"message": "No snapshots found", "results": []}

    # Намираме всички snapshot-и за последните 7 дни
    cur.execute("""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE country=? AND chart_type='top_free'
          AND snapshot_date < ?
        ORDER BY snapshot_date DESC LIMIT ?
    """, (country, latest, lookback_days))
    prev_dates = [r[0] for r in cur.fetchall()]
    if not prev_dates:
        con.close()
        return {"message": "Not enough previous snapshots", "results": []}

    # Текущи
    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts
        WHERE country=? AND chart_type='top_free' AND snapshot_date=?
    """, (country, latest))
    current_data = {r[0]: r for r in cur.fetchall()}

    # Предишни 7 дни
    prev_data = {}
    for d in prev_dates:
        cur.execute("""
            SELECT app_id, rank FROM charts
            WHERE country=? AND chart_type='top_free' AND snapshot_date=?
        """, (country, d))
        for app_id, rank in cur.fetchall():
            if app_id not in prev_data:
                prev_data[app_id] = []
            prev_data[app_id].append(rank)

    results = []

    # Обработка за текущите приложения
    for app_id, row in current_data.items():
        app_name, dev, cat, subcat, rank_now = row[1], row[2], row[3], row[4], row[5]
        if app_id not in prev_data:
            status, rank_prev, rank_change = "NEW", None, None
        else:
            # Средна стойност от предходните 7 дни
            prev_ranks = prev_data[app_id]
            rank_prev = int(sum(prev_ranks) / len(prev_ranks))
            rank_change = rank_prev - rank_now
            if rank_change > 0:
                status = "MOVER UP"
            elif rank_change < 0:
                status = "MOVER DOWN"
            else:
                status = "IN TOP"
        results.append({
            "app_id": app_id,
            "app_name": app_name,
            "developer": dev,
            "category": cat,
            "subcategory": subcat,
            "current_rank": rank_now,
            "previous_rank": rank_prev,
            "delta": rank_change,
            "status": status,
            "country": country
        })

    # Приложения, които са били, но вече ги няма
    for app_id, prev_ranks in prev_data.items():
        if app_id not in current_data:
            rank_prev = int(sum(prev_ranks) / len(prev_ranks))
            results.append({
                "app_id": app_id,
                "app_name": None,
                "developer": None,
                "category": None,
                "subcategory": None,
                "current_rank": None,
                "previous_rank": rank_prev,
                "delta": None,
                "status": "DROPPED",
                "country": country
            })

    con.close()
    return {
        "latest_snapshot": latest,
        "previous_snapshots": prev_dates,
        "total_results": len(results),
        "results": results
    }


# --- Aliases for frontend compatibility ---
@app.get("/compare")
def compare_alias(limit: int = 50, country: str = "US"):
    """Alias for /compare/weekly-full used by frontend."""
    return compare_weekly_full(country=country, lookback_days=7)

@app.get("/reports/weekly")
def compare_report_alias(country: str = "US"):
    """Alias for /compare/reports/weekly."""
    data = compare_weekly_full(country=country, lookback_days=7)
    new_apps = [r for r in data["results"] if r["status"] == "NEW"]
    dropped = [r for r in data["results"] if r["status"] == "DROPPED"]
    return {
        "latest_snapshot": data["latest_snapshot"],
        "new": new_apps,
        "dropped": dropped
    }

