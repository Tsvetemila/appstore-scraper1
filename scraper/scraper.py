# scraper/scraper.py
import requests
import sqlite3
import os
import time
import json
import csv
from datetime import datetime
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

# Държави според брифа
COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU"]

# App категории (без "General")
APP_CATEGORIES = {
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

# Game подкатегории
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
            bundle_id TEXT,
            app_name TEXT,
            developer_name TEXT,
            price REAL,
            currency TEXT,
            rating REAL,
            ratings_count INTEGER,
            raw TEXT,
            PRIMARY KEY (snapshot_date, country, category, subcategory, chart_type, rank)
        )
    """)
    conn.commit()


def insert_rows(conn, rows):
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO charts (
            snapshot_date, country, category, subcategory, chart_type,
            rank, app_id, bundle_id, app_name, developer_name,
            price, currency, rating, ratings_count, raw
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()


def fetch_rss(country: str, genre_id: int):
    url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/{genre_id}/50/apps.json"
    return http_get_json(url)


def enrich_with_lookup(country: str, app_ids: list):
    """Обогатява метаданни през iTunes Lookup (bundleId, price, rating, ...)."""
    out = {}
    for i in range(0, len(app_ids), 50):
        chunk = app_ids[i:i + 50]
        ids_param = ",".join(chunk)
        url = f"https://itunes.apple.com/lookup?id={ids_param}&country={country}"
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
                "raw": json.dumps(r, ensure_ascii=False)
            }
    return out


def export_latest_csv(db_path: str, out_dir: str | Path | None = None) -> Path:
    """Експорт на най-новия снапшот в CSV (без 'raw' колоната, за да е лек файл)."""
    out_dir = Path(out_dir or Path(db_path).parent)
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute("SELECT MAX(snapshot_date) FROM charts")
        snap = cur.fetchone()[0]
        if not snap:
            raise RuntimeError("No snapshots in DB; nothing to export.")

        cur.execute("""
            SELECT snapshot_date, country, category, subcategory, chart_type,
                   rank, app_id, bundle_id, app_name, developer_name,
                   price, currency, rating, ratings_count
            FROM charts
            WHERE snapshot_date = ?
            ORDER BY country, category, COALESCE(subcategory,''), rank
        """, (snap,))
        rows = cur.fetchall()

    out_path = out_dir / f"charts_{snap}.csv"
    header = [
        "snapshot_date","country","category","subcategory","chart_type",
        "rank","app_id","bundle_id","app_name","developer_name",
        "price","currency","rating","ratings_count",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

    print(f"[OK] exported CSV -> {out_path}")
    return out_path


def scrape():
    snapshot_date = datetime.utcnow().date().isoformat()
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    total = 0

    for country in COUNTRIES:
        # App категории
        for cat_name, cat_id in APP_CATEGORIES.items():
            data = fetch_rss(country, cat_id)
            if not data:
                print(f"[INFO] Empty feed for {country}/{cat_name}")
                continue

            apps = data.get("feed", {}).get("results", [])
            app_ids = [str(a["id"]) for a in apps]
            lookup = enrich_with_lookup(country, app_ids)

            rows = []
            for i, app in enumerate(apps, start=1):
                info = lookup.get(str(app["id"]), {})
                rows.append((
                    snapshot_date, country, cat_name.capitalize(), None,
                    "top_free", i, str(app["id"]),
                    info.get("bundle_id"),
                    app.get("name"),
                    app.get("artistName"),
                    info.get("price"),
                    info.get("currency"),
                    info.get("rating"),
                    info.get("ratings_count"),
                    info.get("raw")
                ))
            insert_rows(conn, rows)
            total += len(rows)
            print(f"[INFO] {country} {cat_name} top_free: {len(rows)} apps")

        # Game подкатегории
        for subcat, sub_id in GAME_CATEGORIES.items():
            data = fetch_rss(country, sub_id)
            if not data:
                print(f"[INFO] Empty feed for {country}/Games/{subcat}")
                continue

            apps = data.get("feed", {}).get("results", [])
            app_ids = [str(a["id"]) for a in apps]
            lookup = enrich_with_lookup(country, app_ids)

            rows = []
            for i, app in enumerate(apps, start=1):
                info = lookup.get(str(app["id"]), {})
                rows.append((
                    snapshot_date, country, "Games", subcat.capitalize(),
                    "top_free", i, str(app["id"]),
                    info.get("bundle_id"),
                    app.get("name"),
                    app.get("artistName"),
                    info.get("price"),
                    info.get("currency"),
                    info.get("rating"),
                    info.get("ratings_count"),
                    info.get("raw")
                ))
            insert_rows(conn, rows)
            total += len(rows)
            print(f"[INFO] {country} Games/{subcat} top_free: {len(rows)} apps")

    conn.close()
    print(f"[OK] inserted {total} rows into charts for date {snapshot_date}")

    # Експорт на CSV за най-новия снапшот в същата папка като БД
    export_latest_csv(DB_PATH)


if __name__ == "__main__":
    scrape()
