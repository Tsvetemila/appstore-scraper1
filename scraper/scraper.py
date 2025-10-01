import os
import sys
import json
import time
import sqlite3
import datetime as dt
from typing import Dict, List, Optional
import requests

# ---------- КОНФИГ ----------

# Път към базата (същото както в main.py → appstore-api/data/app_data.db)
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "appstore-api", "data", "app_data.db")

# Държави за scrape (ISO кодове от App Store)
COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU"]

# Работим само с Top Free (по брифа)
CHART_TYPE = "top_free"
LIMIT = 50

# Retry настройки
HTTP_RETRIES = 3
HTTP_TIMEOUT = 20  # сек

# ---------- Помощни функции ----------

def http_get_json(url: str) -> Optional[dict]:
    """GET към URL с прости retry-та; връща JSON или None."""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "charts-bot/1.0"})
            if r.status_code == 200:
                return r.json()
            else:
                print(f"[WARN] {r.status_code} from {url}")
        except Exception as e:
            print(f"[WARN] attempt {attempt}/{HTTP_RETRIES} failed for {url}: {e}")
        time.sleep(1.2 * attempt)
    return None

def ensure_schema():
    """Създава таблицата charts, ако я няма."""
    schema = """
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS charts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date   TEXT    NOT NULL,
        country         TEXT    NOT NULL,
        chart_type      TEXT    NOT NULL,
        category        TEXT,
        subcategory     TEXT,
        rank            INTEGER NOT NULL,
        app_id          TEXT    NOT NULL,
        bundle_id       TEXT,
        app_name        TEXT    NOT NULL,
        developer_name  TEXT,
        price           REAL,
        currency        TEXT,
        rating          REAL,
        ratings_count   INTEGER,
        fetched_at      TEXT DEFAULT (datetime('now')),
        raw             TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_charts_context_rank
    ON charts (snapshot_date, country, chart_type, category, subcategory, rank);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_charts_context_app
    ON charts (snapshot_date, country, chart_type, category, subcategory, app_id);
    CREATE INDEX IF NOT EXISTS idx_charts_date ON charts (snapshot_date);
    CREATE INDEX IF NOT EXISTS idx_charts_app_date ON charts (app_id, snapshot_date);
    CREATE INDEX IF NOT EXISTS idx_charts_ctx_date ON charts (country, chart_type, category, subcategory, snapshot_date);
    """
    con = sqlite3.connect(DB_PATH)
    con.executescript(schema)
    con.commit()
    con.close()

def insert_rows(rows: List[Dict]):
    """INSERT OR REPLACE към charts."""
    if not rows:
        return
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO charts
        (snapshot_date, country, chart_type, category, subcategory, rank,
         app_id, bundle_id, app_name, developer_name, price, currency,
         rating, ratings_count, fetched_at, raw)
        VALUES
        (:snapshot_date, :country, :chart_type, :category, :subcategory, :rank,
         :app_id, :bundle_id, :app_name, :developer_name, :price, :currency,
         :rating, :ratings_count, :fetched_at, :raw)
    """, rows)
    con.commit()
    con.close()

# ---------- Източници на данни ----------

def fetch_top_from_rss(country: str, chart_type: str, limit: int) -> List[Dict]:
    """Дърпа топ N от официалния RSS/JSON фийд на Apple Marketing Tools."""
    rss_type = chart_type.replace("_", "-")  # top_free -> top-free
    url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/{rss_type}/{limit}/apps.json"
    data = http_get_json(url) or {}
    results = []
    for i, item in enumerate(data.get("feed", {}).get("results", []), start=1):
        results.append({
            "rank": i,
            "app_id": str(item.get("id") or ""),
            "app_name": item.get("name") or "",
            "developer_name": item.get("artistName") or "",
            "bundle_id": None,
            "price": None,
            "currency": None,
            "rating": None,
            "ratings_count": None,
            "raw": json.dumps(item, ensure_ascii=False),
        })
    return results

def enrich_with_lookup(country: str, app_ids: List[str], batch_size: int = 50) -> Dict[str, Dict]:
    """Допълва информация за приложенията през iTunes Lookup API."""
    out = {}
    for i in range(0, len(app_ids), batch_size):
        chunk = app_ids[i:i+batch_size]
        ids_param = ",".join(chunk)
        url = f"https://itunes.apple.com/lookup?id={ids_param}&country={country}"
        data = http_get_json(url) or {}
        for r in (data.get("results") or []):
            app_id = str(r.get("trackId") or "")
            if not app_id:
                continue
            out[app_id] = {
                "bundle_id": r.get("bundleId"),
                "price": r.get("price"),
                "currency": r.get("currency"),
                "rating": r.get("averageUserRating"),
                "ratings_count": r.get("userRatingCount"),
                "primaryGenreName": r.get("primaryGenreName"),
                "genres": r.get("genres"),
            }
    return out

# ---------- Главна логика ----------

def run_once():
    ensure_schema()

    today = dt.date.today().strftime("%Y-%m-%d")
    fetched_at = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    all_rows: List[Dict] = []

    for country in COUNTRIES:
        print(f"[INFO] Fetch {country} {CHART_TYPE} (top {LIMIT})")

        base_rows = fetch_top_from_rss(country, CHART_TYPE, limit=LIMIT)
        if not base_rows:
            print(f"[WARN] Empty feed for {country}")
            continue

        lookup_map = enrich_with_lookup(country, [r["app_id"] for r in base_rows])

        for r in base_rows:
            info = lookup_map.get(r["app_id"], {})
            genre = info.get("primaryGenreName")
            subcategory = None
            if genre == "Games" and info.get("genres") and len(info["genres"]) > 1:
                subcategory = info["genres"][1]

            row = {
                "snapshot_date": today,
                "country": country,
                "chart_type": CHART_TYPE,
                "category": genre,
                "subcategory": subcategory,
                "rank": r["rank"],
                "app_id": r["app_id"],
                "bundle_id": info.get("bundle_id"),
                "app_name": r["app_name"],
                "developer_name": r["developer_name"],
                "price": info.get("price"),
                "currency": info.get("currency"),
                "rating": info.get("rating"),
                "ratings_count": info.get("ratings_count"),
                "fetched_at": fetched_at,
                "raw": r["raw"],
            }
            all_rows.append(row)

        time.sleep(0.6)

    if not all_rows:
        print("[WARN] nothing to insert")
        return

    insert_rows(all_rows)
    print(f"[OK] inserted {len(all_rows)} rows into charts for date {today}")

# ---------- CLI ----------

if __name__ == "__main__":
    try:
        run_once()
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(1)
