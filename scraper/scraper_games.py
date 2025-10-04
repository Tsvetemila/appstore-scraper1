# scraper_games.py
import requests, sqlite3, os, time, json
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU", "IT", "CA"]

HTTP_TIMEOUT, HTTP_RETRIES = 10, 3


def http_get_json(url: str):
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "charts-bot/1.0"})
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                print(f"[INFO] 404 Not found: {url}")
                return None
            print(f"[WARN] HTTP {r.status_code} from {url}")
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
            bundle_id TEXT,
            app_name TEXT,
            developer_name TEXT,
            price REAL,
            currency TEXT,
            rating REAL,
            ratings_count INTEGER,
            genre_id TEXT,
            raw TEXT,
            PRIMARY KEY (snapshot_date, country, category, subcategory, chart_type, rank)
        )
    """)
    cur.execute("PRAGMA table_info(charts)")
    cols = [r[1] for r in cur.fetchall()]
    if "genre_id" not in cols:
        print("[INFO] Adding missing column genre_id to charts table.")
        cur.execute("ALTER TABLE charts ADD COLUMN genre_id TEXT;")
    conn.commit()


def insert_rows(conn, rows):
    conn.executemany("""
        INSERT OR REPLACE INTO charts (
            snapshot_date, country, category, subcategory, chart_type, rank,
            app_id, bundle_id, app_name, developer_name, price, currency,
            rating, ratings_count, genre_id, raw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()


def fetch_top_free_games(country: str):
    url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/games/top-free/50/apps.json"
    return http_get_json(url)


def enrich_with_lookup(country: str, app_ids: list):
    out = {}
    for i in range(0, len(app_ids), 50):
        chunk = app_ids[i:i + 50]
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
                "genre_id": str(r.get("primaryGenreId")),
                "category": "Games",
                "subcategory": (r.get("genres") or ["Games"])[0],
                "raw": json.dumps(r, ensure_ascii=False),
            }
    return out


def scrape_games():
    snapshot_date = datetime.utcnow().date().isoformat()
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    total = 0

    for country in COUNTRIES:
        data = fetch_top_free_games(country)
        if not data or not data.get("feed", {}).get("results"):
            print(f"[INFO] Empty top-free games feed for {country}")
            continue

        apps = data["feed"]["results"]
        app_ids = [str(a["id"]) for a in apps]
        lookup = enrich_with_lookup(country, app_ids)

        rows = []
        for i, app in enumerate(apps, start=1):
            info = lookup.get(str(app["id"]), {})
            rows.append((
                snapshot_date,
                country,
                info.get("category") or "Games",
                info.get("subcategory"),
                "top_free",
                i,
                str(app["id"]),
                info.get("bundle_id"),
                app.get("name"),
                app.get("artistName"),
                info.get("price"),
                info.get("currency"),
                info.get("rating"),
                info.get("ratings_count"),
                info.get("genre_id"),
                info.get("raw")
            ))

        insert_rows(conn, rows)
        total += len(rows)
        print(f"[INFO] {country} top_free games: {len(rows)}")

    conn.close()
    print(f"[OK] GAMES inserted {total} rows for date {snapshot_date}")


if __name__ == "__main__":
    scrape_games()
