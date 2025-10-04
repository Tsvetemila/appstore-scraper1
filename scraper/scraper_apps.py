# scraper_apps.py
import requests, sqlite3, os, time, json
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU"]

APP_CATEGORIES = {
    "books": 6018, "business": 6000, "developer-tools": 6026, "education": 6017,
    "entertainment": 6016, "finance": 6015, "food-drink": 6023, "graphics-design": 6027,
    "health-fitness": 6013, "kids": 6061, "lifestyle": 6012, "magazines-newspapers": 6021,
    "medical": 6020, "music": 6011, "navigation": 6010, "news": 6009, "photo-video": 6008,
    "productivity": 6007, "reference": 6006, "shopping": 6024, "social-networking": 6005,
    "sports": 6004, "travel": 6003, "utilities": 6002, "weather": 6001,
}

HTTP_TIMEOUT, HTTP_RETRIES = 10, 3

def http_get_json(url: str):
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "charts-bot/1.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                print(f"[INFO] 404 no chart at {url}")
                return None
            print(f"[WARN] {r.status_code} from {url}")
        except Exception as e:
            print(f"[WARN] attempt {attempt}/{HTTP_RETRIES} failed for {url}: {e}")
        time.sleep(1.2 * attempt)
    return None

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS charts (
            snapshot_date TEXT, country TEXT, category TEXT, subcategory TEXT,
            chart_type TEXT, rank INTEGER, app_id TEXT, bundle_id TEXT,
            app_name TEXT, developer_name TEXT, price REAL, currency TEXT,
            rating REAL, ratings_count INTEGER, raw TEXT,
            PRIMARY KEY (snapshot_date, country, category, subcategory, chart_type, rank)
        )
    """)
    conn.commit()

def insert_rows(conn, rows):
    conn.executemany("""
        INSERT OR REPLACE INTO charts (
            snapshot_date, country, category, subcategory, chart_type, rank,
            app_id, bundle_id, app_name, developer_name, price, currency,
            rating, ratings_count, raw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()

def fetch_rss(country: str, genre_id: int):
    """Fetch data from Apple RSS with fallback if genre feed is empty."""
    url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/{genre_id}/50/apps.json"
    data = http_get_json(url)
    # fallback if empty
    if not data or not data.get("feed", {}).get("results"):
        fallback_url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/50/apps.json"
        print(f"[FALLBACK] Empty feed for {country}/{genre_id}, switching to general feed.")
        data = http_get_json(fallback_url)
    return data

def enrich_with_lookup(country: str, app_ids: list):
    out = {}
    for i in range(0, len(app_ids), 50):
        chunk = app_ids[i:i+50]
        url = f"https://itunes.apple.com/lookup?id={','.join(chunk)}&country={country}"
        data = http_get_json(url) or {}
        for r in data.get("results", []):
            app_id = str(r.get("trackId") or "")
            if not app_id:
                continue
            out[app_id] = {
                "bundle_id": r.get("bundleId"),
                "price": r.get("price"),
                "currency": r.get("currency"),
                "rating": r.get("averageUserRating"),
                "ratings_count": r.get("userRatingCount"),
                "raw": json.dumps(r, ensure_ascii=False),
            }
    return out

def scrape_apps():
    snapshot_date = datetime.utcnow().date().isoformat()
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    total = 0

    for country in COUNTRIES:
        for cat_name, cat_id in APP_CATEGORIES.items():
            data = fetch_rss(country, cat_id)
            if not data:
                print(f"[INFO] Empty feed for {country}/{cat_name}")
                continue

            apps = data.get("feed", {}).get("results", [])
            if not apps:
                print(f"[INFO] No apps for {country}/{cat_name}")
                continue

            app_ids = [str(a["id"]) for a in apps]
            lookup = enrich_with_lookup(country, app_ids)

            rows = []
            for i, app in enumerate(apps, start=1):
                info = lookup.get(str(app["id"]), {})
                rows.append((
                    snapshot_date, country, cat_name.capitalize(), None,
                    "top_free", i, str(app["id"]),
                    info.get("bundle_id"), app.get("name"), app.get("artistName"),
                    info.get("price"), info.get("currency"),
                    info.get("rating"), info.get("ratings_count"),
                    info.get("raw")
                ))
            if rows:
                insert_rows(conn, rows)
                total += len(rows)
                print(f"[INFO] {country} {cat_name} top_free: {len(rows)}")

    conn.close()
    print(f"[OK] APPS inserted {total} rows for date {snapshot_date}")

if __name__ == "__main__":
    scrape_apps()
