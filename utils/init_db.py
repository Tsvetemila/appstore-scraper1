# utils/init_db.py
import os, sqlite3

DB_PATH = os.environ.get("DB_PATH", "appstore-api/data/app_data.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS apps(
    app_id TEXT PRIMARY KEY,
    bundle_id TEXT,
    name TEXT,
    developer TEXT
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS charts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT,
    country TEXT,
    chart_type TEXT,
    category TEXT,
    subcategory TEXT,
    rank INTEGER,
    app_id TEXT
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS snapshots(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT,
    country TEXT,
    created_at TEXT
);
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS compare_results(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date TEXT,
    country TEXT,
    chart_type TEXT,
    category TEXT,
    app_id TEXT,
    current_rank INTEGER,
    previous_rank INTEGER,
    delta INTEGER,
    status TEXT
);
""")
conn.commit()
conn.close()
print(f"DB schema ensured at {DB_PATH}")
