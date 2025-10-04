# scraper_games.py
import os, time, json, sqlite3, requests
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU", "IT", "CA"]

# GAME поджанрове (genre_id)
GAME_CATEGORIES = {
    "action": 7001, "adventure": 7002, "board": 7004, "card": 7005,
    "casino": 7006, "casual": 7007, "family": 7008, "music": 7011,
    "puzzle": 7012, "racing": 7013, "role-playing": 7014, "simulation": 7015,
    "sports": 7016, "strategy": 7017, "trivia": 7018, "word": 7019,
}

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
        time.sleep(1.1 * attempt)
    return None

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS charts (
            snapshot_date TEXT,
            country       TEXT,
            category      TEXT,
            subcategory   TEXT,
            chart_type    TEXT,
            rank          INTEGER,
            app_id        TEXT,
            bundle_id     TEXT,
            app_name      TEXT,
            developer_name TEXT,
            price         REAL,
            currency      TEXT,
            rating        REAL,
            ratings_count INTEGER,
            genre_id      TEXT,
            raw           TEXT,
            PRIMARY KEY (snapshot_date, country, category, subcategory, chart_type, rank)
        )
    """)
    cur.execute("PRAGMA table_info(charts)")
    cols = [c[1] for c in cur.fetchall()]
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

# ---- FEEDS ----
def fetch_marketingtools_genre(country: str, genre_id: int):
    url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/{genre_id}/50/apps.json"
    return http_get_json(url)

def fetch_itunes_genre(country: str, genre_id: int):
    url = f"https://itunes.apple.com/{country.lower()}/rss/topfreeapplications/limit=50/genre={genre_id}/json"
    return http_get_json(url)

def parse_marketingtools_results(data):
    res = []
    for i, a in enumerate(data.get("feed", {}).get("results", []), start=1):
        res.append({"rank": i, "id": str(a.get("id")), "name": a.get("name"), "artistName": a.get("artistName")})
    return res

def parse_itunes_results(data):
    res = []
    entries = data.get("feed", {}).get("entry", []) or []
    for i, e in enumerate(entries, start=1):
        id_url = ((e.get("id") or {}).get("label") or "")
        app_id = ""
        if "/id" in id_url:
            try: app_id = id_url.split("/id", 1)[1].split("?", 1)[0]
            except Exception: app_id = ""
        name   = ((e.get("im:name") or {}).get("label")) or (e.get("title") or {}).get("label")
        artist = ((e.get("im:artist") or {}).get("label")) or ""
        res.append({"rank": i, "id": app_id, "name": name, "artistName": artist})
    return res

def fetch_genre_top50(country: str, genre_id: int):
    data  = fetch_marketingtools_genre(country, genre_id)
    items = parse_marketingtools_results(data) if data else []
    if items: return items, "marketingtools"
    data  = fetch_itunes_genre(country, genre_id)
    items = parse_itunes_results(data) if data else []
    if items: print(f"[FALLBACK] Using iTunes RSS for {country}/Games/{genre_id}")
    return items, "itunes"

def enrich_with_lookup(country: str, app_ids: list):
    out = {}
    for i in range(0, len(app_ids), 50):
        chunk = [x for x in app_ids[i:i+50] if x]
        if not chunk: continue
        url = f"https://itunes.apple.com/lookup?id={','.join(chunk)}&country={country}"
        data = http_get_json(url) or {}
        for r in data.get("results", []):
            app_id = str(r.get("trackId") or "")
            if not app_id: continue
            # genres е списък, първият често е поджанрът (напр. "Puzzle")
            genres = r.get("genres") or []
            subcat = None
            if genres:
                # махаме "Games" ако е първи
                subcat = genres[0] if genres[0].lower() != "games" else (genres[1] if len(genres) > 1 else "Games")
            out[app_id] = {
                "bundle_id": r.get("bundleId"),
                "price": r.get("price"),
                "currency": r.get("currency"),
                "rating": r.get("averageUserRating"),
                "ratings_count": r.get("userRatingCount"),
                "genre_id": str(r.get("primaryGenreId")),
                "subcategory": subcat,
                "raw": json.dumps(r, ensure_ascii=False),
            }
    return out

def scrape_games():
    snapshot_date = datetime.utcnow().date().isoformat()
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    total = 0

    for country in COUNTRIES:
        for sub_slug, genre_id in GAME_CATEGORIES.items():
            items, src = fetch_genre_top50(country, genre_id)
            if not items:
                print(f"[INFO] Empty feed for {country}/Games/{genre_id} ({sub_slug})")
                continue

            app_ids = [it["id"] for it in items if it.get("id")]
            lookup  = enrich_with_lookup(country, app_ids)

            rows = []
            for it in items:
                info = lookup.get(str(it["id"]), {})
                rows.append((
                    snapshot_date, country,
                    "Games", sub_slug.replace("-", " ").title(),
                    "top_free", it["rank"], str(it["id"]),
                    info.get("bundle_id"), it.get("name"), it.get("artistName"),
                    info.get("price"), info.get("currency"),
                    info.get("rating"), info.get("ratings_count"),
                    info.get("genre_id"), info.get("raw"),
                ))

            insert_rows(conn, rows)
            total += len(rows)
            print(f"[INFO] {country} Games/{sub_slug} top_free ({src}): {len(rows)} rows")

    conn.close()
    print(f"[OK] GAMES inserted {total} rows for date {snapshot_date}")

if __name__ == "__main__":
    scrape_games()
