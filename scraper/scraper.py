import requests
import sqlite3
import os
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

# Държави
COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU"]

# Основни категории
APP_CATEGORIES = {
    "all": None,  # General Top 50
    "books": 6018,
    "business": 6000,
    "developer-tools": 6026,
    "education": 6017,
    "entertainment": 6016,
    "finance": 6015,
    "food-drink": 6023,
    "graphics-design": 6027,
    "health-fitness": 6013,
    "kids": 6061,
    "lifestyle": 6012,
    "magazines-newspapers": 6021,
    "medical": 6020,
    "music": 6011,
    "navigation": 6010,
    "news": 6009,
    "photo-video": 6008,
    "productivity": 6007,
    "reference": 6006,
    "shopping": 6024,
    "social-networking": 6005,
    "sports": 6004,
    "travel": 6003,
    "utilities": 6002,
    "weather": 6001,
}

# Подкатегории на Games
GAME_CATEGORIES = {
    "action": 7001,
    "adventure": 7002,
    "board": 7004,
    "card": 7005,
    "casino": 7006,
    "casual": 7007,
    "family": 7008,
    "music": 7011,
    "puzzle": 7012,
    "racing": 7013,
    "role-playing": 7014,
    "simulation": 7015,
    "sports": 7016,
    "strategy": 7017,
    "trivia": 7018,
    "word": 7019,
}

HTTP_TIMEOUT = 10
HTTP_RETRIES = 3

def http_get_json(url: str):
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "charts-bot/1.0"})
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                print(f"[INFO] No chart available at {url}")
                return None
            else:
                print(f"[WARN] {r.status_code} from {url}")
        except Exception as e:
            print(f"[WARN] attempt {attempt}/{HTTP_RETRIES} failed for {url}: {e}")
        time.sleep(1.2 * attempt)
    return None

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS charts (
            snapshot_date TEXT,
            country TEXT,
            category TEXT,
            subcategory TEXT,
            chart_type TEXT,
            rank INTEGER,
            app_id TEXT,
            app_name TEXT,
            PRIMARY KEY (snapshot_date, country, category, subcategory, chart_type, rank)
        )
    """)
    conn.commit()

def insert_rows(conn, rows):
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO charts (
            snapshot_date, country, category, subcategory, chart_type,
            rank, app_id, app_name
        )
        VALUES (?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

def scrape():
    snapshot_date = datetime.utcnow().date().isoformat()
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    total = 0

    for country in COUNTRIES:
        # General + App categories
        for cat_name, cat_id in APP_CATEGORIES.items():
            url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/50/apps.json"
            if cat_id:
                url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/{cat_id}/50/apps.json"

            data = http_get_json(url)
            if not data:
                print(f"[INFO] Empty feed for {country}/{cat_name}")
                continue

            rows = []
            for i, app in enumerate(data.get("feed", {}).get("results", []), start=1):
                rows.append((
                    snapshot_date, country, cat_name.capitalize(), None,
                    "top_free", i, app["id"], app["name"]
                ))
            insert_rows(conn, rows)
            total += len(rows)
            print(f"[INFO] {country} {cat_name} top_free: {len(rows)} apps")

        # Game subcategories
        for subcat, sub_id in GAME_CATEGORIES.items():
            url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/{sub_id}/50/apps.json"
            data = http_get_json(url)
            if not data:
                print(f"[INFO] Empty feed for {country}/Games/{subcat}")
                continue

            rows = []
            for i, app in enumerate(data.get("feed", {}).get("results", []), start=1):
                rows.append((
                    snapshot_date, country, "Games", subcat.capitalize(),
                    "top_free", i, app["id"], app["name"]
                ))
            insert_rows(conn, rows)
            total += len(rows)
            print(f"[INFO] {country} Games/{subcat} top_free: {len(rows)} apps")

    conn.close()
    print(f"[OK] inserted {total} rows into charts for date {snapshot_date}")

if __name__ == "__main__":
    scrape()
