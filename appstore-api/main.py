# appstore-api/main.py
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List, Dict, Any
from pathlib import Path
import os, sqlite3, json, io, csv, glob
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime, timezone

# ------------------------- 1) Download DB from Google Drive -------------------------
def ensure_database_from_drive(force_if_newer: bool = False) -> Dict[str, Any]:
    """
    - If DB is missing: download it.
    - If force_if_newer=True: check Drive modification time and download only if newer than local.
    """
    local_path = "appstore-api/appstore-api/data/app_data.db"
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    info = {"downloaded": False, "path": local_path, "reason": ""}

    if not creds_json or not folder_id:
        info["reason"] = "Missing Google Drive credentials."
        print("⚠️ Missing Google Drive credentials. Skipping DB download.")
        return info

    local_exists = os.path.exists(local_path)

    # If not forcing and local exists — nothing to do
    if local_exists and not force_if_newer:
        info["reason"] = "Local DB present, no force."
        print("✅ Database already exists locally.")
        return info

    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json))
        drive = build("drive", "v3", credentials=creds)
        results = drive.files().list(
            q=f"'{folder_id}' in parents and name='app_data.db' and trashed=false",
            fields="files(id, name, modifiedTime)"
        ).execute()
        files = results.get("files", [])
        if not files:
            info["reason"] = "No app_data.db in Drive folder."
            print("⚠️ No app_data.db found in Drive folder.")
            return info

        meta = files[0]
        remote_mtime = datetime.fromisoformat(meta["modifiedTime"].replace("Z", "+00:00"))

        if local_exists and force_if_newer:
            local_mtime = datetime.fromtimestamp(os.path.getmtime(local_path), tz=timezone.utc)
            if remote_mtime <= local_mtime:
                info["reason"] = "Remote not newer."
                print("ℹ️ Remote DB is not newer; skipping download.")
                return info

        print("⬇️ Downloading database from Google Drive...")
        request = drive.files().get_media(fileId=meta["id"])
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with io.FileIO(local_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"⬇️ Download progress: {int(status.progress() * 100)}%")
        info["downloaded"] = True
        info["reason"] = "Downloaded"
        print(f"✅ Database downloaded to {local_path}")
    except Exception as e:
        info["reason"] = f"Error: {e}"
        print(f"❌ Error downloading DB: {e}")

    return info


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

    # make sure columns exist even on older DBs
    for tbl, col in [
        ("charts", "developer"),
        ("snapshots", "category"),
        ("snapshots", "subcategory"),
        ("snapshots", "data"),
    ]:
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
    conn.row_factory = sqlite3.Row
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
ensure_database_from_drive()  # on cold start only

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "appstore-api" / "data" / "app_data.db"
if not DB_PATH.exists():
    DB_PATH = APP_DIR / "data" / "app_data.db"
if not DB_PATH.exists():
    matches = glob.glob("**/app_data.db", recursive=True)
    if matches:
        DB_PATH = Path(matches[0]).resolve()
print(f"📘 Using DB: {DB_PATH}")

ensure_tables_exist(str(DB_PATH))
populate_derived_tables(str(DB_PATH))

app = FastAPI(title="AppStore Charts API", version="1.4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://appstore-scraper1.vercel.app",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
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
    parts, params = ["chart_type='top_free'"], []
    for col in ("country", "category", "subcategory"):
        val = filters.get(col)
        if val and val != "all":
            parts.append(f"{col}=?")
            params.append(val)
    return " AND ".join(parts), params


def _latest_snapshot_for_country(cur, country: str) -> Optional[str]:
    cur.execute(
        "SELECT MAX(snapshot_date) FROM charts WHERE country=? AND chart_type='top_free'", (country,)
    )
    return cur.fetchone()[0]


# ------------------------- 5) META (filters) -----------------------------------------------
@app.get("/meta")
def get_meta(category: Optional[str] = None):
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
                (category,),
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


# ------------------------- 6) Charts (latest top 50) ---------------------------------------
@app.get("/charts")
def charts(country: str = "US", limit: int = 50):
    con = connect(); cur = con.cursor()
    latest = _latest_snapshot_for_country(cur, country)
    if not latest:
        con.close()
        return {"rows": [], "snapshot_date": None}
    cur.execute("""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts
        WHERE country=? AND chart_type='top_free' AND snapshot_date=?
        ORDER BY rank ASC LIMIT ?
    """, (country, latest, limit))
    rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
    con.close()
    return {"snapshot_date": latest, "rows": rows}


# ------------------------- 7) Weekly compare (last 7 d) ------------------------------------
@app.get("/compare/weekly-full")
def compare_weekly_full(
    country: str = "US",
    lookback_days: int = 7,
    category: Optional[str] = Query(None),
    subcategory: Optional[str] = Query(None),
):
    con = connect(); cur = con.cursor()

    latest = _latest_snapshot_for_country(cur, country)
    if not latest:
        con.close()
        return {"message": "No snapshots found", "results": [], "latest_snapshot": None}

    # previous dates strictly before latest
    cur.execute("""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE country=? AND chart_type='top_free' AND snapshot_date < ?
        ORDER BY snapshot_date DESC LIMIT ?
    """, (country, latest, lookback_days))
    prev_dates = [r[0] for r in cur.fetchall()]
    if not prev_dates:
        con.close()
        return {"message": "Not enough previous snapshots", "results": [], "latest_snapshot": latest}

    # WHERE for optional filters
    where_base, params_base = _where({"country": country, "category": category, "subcategory": subcategory})

    # current snapshot
    cur.execute(f"""
        SELECT app_id, app_name, developer, category, subcategory, rank
        FROM charts
        WHERE {where_base} AND snapshot_date=?
    """, (*params_base, latest))
    current_rows = cur.fetchall()
    current_data = {r["app_id"]: r for r in current_rows}

    # collect previous ranks
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

    # still in top
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

    # dropped
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


# ----- Aliases used by frontend + CSV export ------------------------------------------------
@app.get("/compare")
def compare_alias(
    limit: int = 50,
    country: str = "US",
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    format: Optional[str] = None,
):
    data = compare_weekly_full(country=country, category=category, subcategory=subcategory, lookback_days=7)
    # limit only for JSON
    if (format or "").lower() != "csv":
        data["results"] = sorted(data["results"], key=lambda r: (r["current_rank"] is None, r["current_rank"] or 999))[:limit]
        return data

    # CSV export
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["country", "category", "subcategory", "app", "developer", "current_rank", "previous_rank", "delta", "status", "app_id"])
    for r in data["results"]:
        writer.writerow([
            r["country"], r["category"], r["subcategory"], r["app_name"], r.get("developer") or "",
            r["current_rank"], r["previous_rank"], r["delta"], r["status"], r["app_id"]
        ])
    return Response(content=output.getvalue(), media_type="text/csv")


@app.get("/reports/weekly")
def weekly_report(
    country: str = "US",
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    format: Optional[str] = None,
):
    data = compare_weekly_full(country=country, category=category, subcategory=subcategory, lookback_days=7)

    new_apps = [
        {"rank": r["current_rank"], "app_id": r["app_id"], "app_name": r["app_name"], "developer_name": r.get("developer") or ""}
        for r in data["results"] if r["status"] == "NEW"
    ]
    dropped_apps = [
        {"rank": r["previous_rank"], "app_id": r["app_id"], "app_name": r["app_name"], "developer_name": r.get("developer") or ""}
        for r in data["results"] if r["status"] == "DROPPED"
    ]

    if (format or "").lower() == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["bucket", "rank", "app", "developer", "app_id"])
        for a in new_apps:
            writer.writerow(["NEW", a["rank"], a["app_name"], a["developer_name"], a["app_id"]])
        for a in dropped_apps:
            writer.writerow(["DROPPED", a["rank"], a["app_name"], a["developer_name"], a["app_id"]])
        return Response(content=output.getvalue(), media_type="text/csv")

    return {"latest_snapshot": data["latest_snapshot"], "new": new_apps, "dropped": dropped_apps}


# ------------------------- 8) Admin: refresh DB (manual) -----------------------------------
@app.get("/admin/refresh")
def admin_refresh():
    info = ensure_database_from_drive(force_if_newer=True)
    # always (re)check structure afterwards
    ensure_tables_exist(str(DB_PATH))
    populate_derived_tables(str(DB_PATH))

    # return latest snapshot per any country with most recent
    con = connect(); cur = con.cursor()
    cur.execute("SELECT MAX(snapshot_date) FROM charts")
    latest = cur.fetchone()[0]
    con.close()
    return {"message": f"Refresh: {info.get('reason','done')}", "downloaded": info.get("downloaded", False), "latest_snapshot": latest}


# ------------------------- 9) History View (Re-Entry Tracker) ----------------------------
@app.get("/history")
def history_view(
    country: str = "US",
    lookback_days: int = 7,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
):
    con = connect()
    cur = con.cursor()

    # Последните 7 snapshot дати
    cur.execute("""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE country=? AND chart_type='top_free'
        ORDER BY snapshot_date DESC LIMIT ?
    """, (country, lookback_days))
    dates = [r[0] for r in cur.fetchall()]
    if len(dates) < 2:
        con.close()
        return {"message": "Not enough data for history.", "results": []}

    results = []
    dates = sorted(dates)  # ascending order for timeline

    for i in range(1, len(dates)):
        prev_day, curr_day = dates[i - 1], dates[i]

        # текущи и предишни данни
        cur.execute("""
            SELECT app_id, app_name, developer, rank
            FROM charts
            WHERE country=? AND chart_type='top_free' AND snapshot_date=?
        """, (country, prev_day))
        prev_rows = {r["app_id"]: dict(r) for r in cur.fetchall()}

        cur.execute("""
            SELECT app_id, app_name, developer, rank
            FROM charts
            WHERE country=? AND chart_type='top_free' AND snapshot_date=?
        """, (country, curr_day))
        curr_rows = {r["app_id"]: dict(r) for r in cur.fetchall()}

        # текущи APP_ID-та
        prev_ids, curr_ids = set(prev_rows.keys()), set(curr_rows.keys())

        # нови
        new_apps = curr_ids - prev_ids
        for app_id in new_apps:
            rank_now = curr_rows[app_id]["rank"]
            replaced = next((p for p, d in prev_rows.items() if d["rank"] == rank_now), None)
            replaced_name = prev_rows[replaced]["app_name"] if replaced else None
            results.append({
                "date": curr_day,
                "status": "NEW",
                "app_id": app_id,
                "app_name": curr_rows[app_id]["app_name"],
                "developer": curr_rows[app_id]["developer"],
                "current_rank": rank_now,
                "replaced_app": replaced_name,
                "replaced_app_id": replaced,
            })

        # отпаднали
        dropped_apps = prev_ids - curr_ids
        for app_id in dropped_apps:
            results.append({
                "date": curr_day,
                "status": "DROPPED",
                "app_id": app_id,
                "app_name": prev_rows[app_id]["app_name"],
                "developer": prev_rows[app_id]["developer"],
                "previous_rank": prev_rows[app_id]["rank"],
            })

        # върнали се (re-entry)
        for app_id in curr_ids & prev_ids:
            if app_id in dropped_apps:
                continue
            prev_rank = prev_rows[app_id]["rank"]
            curr_rank = curr_rows[app_id]["rank"]
            if prev_rank != curr_rank:
                results.append({
                    "date": curr_day,
                    "status": "MOVED",
                    "app_id": app_id,
                    "app_name": curr_rows[app_id]["app_name"],
                    "developer": curr_rows[app_id]["developer"],
                    "previous_rank": prev_rank,
                    "current_rank": curr_rank,
                })

        # търсим Re-entry (липсвал ден, после се е върнал)
        if i > 1:
            prev_prev_day = dates[i - 2]
            cur.execute("""
                SELECT app_id FROM charts
                WHERE country=? AND chart_type='top_free' AND snapshot_date=?
            """, (country, prev_prev_day))
            prev_prev_ids = set(r[0] for r in cur.fetchall())
            reentries = (curr_ids & prev_prev_ids) - prev_ids
            for app_id in reentries:
                rank_now = curr_rows[app_id]["rank"]
                results.append({
                    "date": curr_day,
                    "status": "RE-ENTRY",
                    "app_id": app_id,
                    "app_name": curr_rows[app_id]["app_name"],
                    "developer": curr_rows[app_id]["developer"],
                    "current_rank": rank_now,
                })

    con.close()
    return {"country": country, "days": dates, "total_events": len(results), "results": results}
