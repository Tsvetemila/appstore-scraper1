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

# ------------------------- 1) Download DB from Google Drive (once) -------------------------
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


# ------------------------- 2) Ensure tables/columns exist ----------------------------------
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

    # make sure some columns exist even on old DBs
    for tbl, col in [("charts", "developer"), ("snapshots", "category"), ("snapshots", "subcategory"), ("snapshots", "data")]:
        cur.execute(f"PRAGMA table_info({tbl});")
        cols = [c[1] for c in cur.fetchall()]
        if col not in cols:
            print(f"⚙️ Adding missing column '{col}' to {tbl}...")
            cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} TEXT;")
            con.commit()

    con.close()
    print("✅ Database structure verified.")


# ------------------------- 3) Populate derived tables (first run only) ---------------------
def populate_derived_tables(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM charts")
    if cur.fetchone()[0] == 0:
        print("⚠️ Charts table empty — nothing to populate.")
        conn.close()
        return

    # apps
    cur.execute("SELECT COUNT(*) FROM apps")
    if cur.fetchone()[0] == 0:
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

    # snapshots (one per date+country+category+subcategory)
    cur.execute("SELECT COUNT(*) FROM snapshots")
    if cur.fetchone()[0] == 0:
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


# ------------------------- 4) Bootstrap DB & app -------------------------------------------
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

app = FastAPI(title="AppStore Charts API", version="1.3")

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


# ------------------------- helpers ----------------------------------------------------------
def _where(filters: Dict[str, Optional[str]]) -> (str, List[Any]):
    """Builds WHERE piece and params for optional filters."""
    parts, params = ["chart_type='top_free'"], []
    for col in ("country", "category", "subcategory"):
        val = filters.get(col)
        if val and val != "all":
            parts.append(f"{col}=?")
            params.append(val)
    return " AND ".join(parts), params


# ------------------------- 5) META (for filters) -------------------------------------------
@app.get("/meta")
def get_meta(category: Optional[str] = None):
    """
    Returns dynamic filter values:
      - countries (always all)
      - categories (always all)
      - subcategories (all OR only for the selected category if provided)
    """
    con = connect()
    cur = con.cursor()
    data = {}
    try:
        cur.execute("SELECT DISTINCT country FROM charts WHERE country IS NOT NULL ORDER BY country")
        data["countries"] = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT category FROM charts WHERE category IS NOT NULL ORDER BY category")
        data["categories"] = [r[0] for r in cur.fetchall()]

        if category and category != "all":
            cur.execute(
                "SELECT DISTINCT subcategory FROM charts WHERE category=? AND subcategory IS NOT NULL ORDER BY subcategory",
                (category,)
            )
        else:
            cur.execute("SELECT DISTINCT subcategory FROM charts WHERE subcategory IS NOT NULL ORDER BY subcategory")
        data["subcategories"] = [r[0] for r in cur.fetchall()]
    except Exception as e:
        print(f"⚠️ Error in /meta: {e}")
        data = {"countries": [], "categories": [], "subcategories": []}
    finally:
        con.close()
    return data


# ------------------------- 6) Current Top 50 (for a country) -------------------------------
@app.get("/charts")
def charts(country: str = "US", limit: int = 50):
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


# ------------------------- 7) Weekly compare (last 7 days) ---------------------------------
@app.get("/compare/weekly-full")
def compare_weekly_full(
    country: str = "US",
    lookback_days: int = 7,
    category: Optional[str] = Query(None),
    subcategory: Optional[str] = Query(None),
):
    """
    Compares the latest snapshot against ALL snapshots from the last `lookback_days`.
    Filters by country/category/subcategory when provided.
    Returns rows with: current_rank, previous_rank (avg of last days), delta and status:
      NEW / DROPPED / MOVER UP / MOVER DOWN / IN TOP
    """
    con = connect(); cur = con.cursor()

    # latest snapshot date (per country)
    cur.execute("""
        SELECT MAX(snapshot_date) FROM charts
        WHERE country=? AND chart_type='top_free'
    """, (country,))
    latest = cur.fetchone()[0]
    if not latest:
        con.close()
        return {"message": "No snapshots found", "results": []}

    # previous dates (strictly before latest)
    cur.execute("""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE country=? AND chart_type='top_free' AND snapshot_date < ?
        ORDER BY snapshot_date DESC LIMIT ?
    """, (country, latest, lookback_days))
    prev_dates = [r[0] for r in cur.fetchall()]
    if not prev_dates:
        con.close()
        return {"message": "Not enough previous snapshots", "results": []}

    # WHERE for optional filters (without date)
    where_base, params_base = _where({"country": country, "category": category, "subcategory": subcategory})

    # current snapshot rows
    cur.execute(f"""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts
        WHERE {where_base} AND snapshot_date=?
    """, (*params_base, latest))
    current_rows = cur.fetchall()
    current_data = {r["app_id"]: r for r in current_rows}

    # previous 7 days data: ranks + last known app_name/developer
    prev_data: Dict[str, Dict[str, Any]] = {}
    for d in prev_dates:
        cur.execute(f"""
            SELECT app_id, app_name, developer, category, subcategory, rank
            FROM charts
            WHERE {where_base} AND snapshot_date=?
        """, (*params_base, d))
        for row in cur.fetchall():
            app_id = row["app_id"]
            if app_id not in prev_data:
                prev_data[app_id] = {
                    "ranks": [],
                    "app_name": row["app_name"],
                    "developer": row["developer"],
                    "category": row["category"],
                    "subcategory": row["subcategory"],
                }
            prev_data[app_id]["ranks"].append(row["rank"])

    results: List[Dict[str, Any]] = []

    # apps that are in the current top
    for app_id, row in current_data.items():
        rank_now = row["rank"]
        app_name = row["app_name"]
        dev = row["developer"]
        cat = row["category"]
        subcat = row["subcategory"]

        if app_id not in prev_data:
            status, rank_prev, rank_change = "NEW", None, None
        else:
            ranks = prev_data[app_id]["ranks"]
            rank_prev = int(sum(ranks) / len(ranks))
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
            "country": country,
        })

    # apps that dropped out
    for app_id, pdata in prev_data.items():
        if app_id not in current_data:
            ranks = pdata["ranks"]
            rank_prev = int(sum(ranks) / len(ranks))
            results.append({
                "app_id": app_id,
                "app_name": pdata["app_name"],
                "developer": pdata["developer"],
                "category": pdata["category"],
                "subcategory": pdata["subcategory"],
                "current_rank": None,
                "previous_rank": rank_prev,
                "delta": None,
                "status": "DROPPED",
                "country": country,
            })

    con.close()
    return {
        "latest_snapshot": latest,
        "previous_snapshots": prev_dates,
        "total_results": len(results),
        "results": results
    }


# ----- Aliases used by frontend -------------------------------------------------------------
@app.get("/compare")
def compare_alias(
    limit: int = 50,
    country: str = "US",
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
):
    return compare_weekly_full(country=country, category=category, subcategory=subcategory, lookback_days=7)


@app.get("/reports/weekly")
def weekly_report(
    country: str = "US",
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    format: Optional[str] = None,
):
    data = compare_weekly_full(country=country, category=category, subcategory=subcategory, lookback_days=7)

    new_apps = [
        {
            "rank": r["current_rank"],
            "app_id": r["app_id"],
            "app_name": r["app_name"],
            "developer_name": r["developer"],
        }
        for r in data["results"] if r["status"] == "NEW"
    ]

    dropped_apps = [
        {
            "rank": r["previous_rank"],
            "app_id": r["app_id"],
            "app_name": r["app_name"],
            "developer_name": r["developer"],
        }
        for r in data["results"] if r["status"] == "DROPPED"
    ]

    # optional CSV export (single file with a "bucket" column)
    if (format or "").lower() == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["bucket", "rank", "app", "developer", "app_id"])
        for a in new_apps:
            writer.writerow(["NEW", a["rank"], a["app_name"], a["developer_name"], a["app_id"]])
        for a in dropped_apps:
            writer.writerow(["DROPPED", a["rank"], a["app_name"], a["developer_name"], a["app_id"]])
        return Response(content=output.getvalue(), media_type="text/csv")

    return {
        "latest_snapshot": data["latest_snapshot"],
        "new": new_apps,
        "dropped": dropped_apps
    }
