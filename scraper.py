import os
import sys
import csv
import glob
import json
import time
import sqlite3
import datetime as dt
from pathlib import Path
from typing import List, Dict

import requests
import pandas as pd

# ----------------------------
# Конфигурации (можеш да ги променяш):
# ----------------------------

# Държави (ISO3166-1 alpha-2)
COUNTRIES = os.getenv("COUNTRIES", "US,GB,DE,FR,IT,ES,JP,BG").split(",")

# Категории (Apple RSS slug-ове). "overall" е общият топ без категория.
# Списъкът е подбран да покрие масовите секции от App Store.
CATEGORIES = os.getenv("CATEGORIES", ",".join([
    "overall",
    "games",
    "music",
    "photo-and-video",
    "entertainment",
    "social-networking",
    "shopping",
    "productivity",
    "travel",
    "sports",
    "health-and-fitness",
    "finance",
    "books",
    "education",
    "news",
    "navigation",
    "weather",
    "medical",
    "lifestyle",
    "food-and-drink",
    "utilities",
    "reference"
])).split(",")

# Колко позиции дърпаме (50 според заданието)
LIMIT = int(os.getenv("LIMIT", "50"))

# Ретраи при временни грешки
RETRIES = 3
SLEEP_BETWEEN_RETRIES = 2.0

# Имена на файлове/папки
DATA_DIR = Path("data")
DB_PATH = Path("appstore_charts.db")

# ----------------------------

def rss_url(country: str, category: str, limit: int = LIMIT) -> str:
    base = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free"
    if category.lower() == "overall":
        return f"{base}/{limit}.json"
    else:
        return f"{base}/{category.lower()}/{limit}.json"

def fetch_one(country: str, category: str) -> pd.DataFrame:
    url = rss_url(country, category)
    last_exc = None
    for _ in range(RETRIES):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                results = data.get("feed", {}).get("results", [])

                rows = []
                for i, app in enumerate(results, start=1):
                    rows.append({
                        "snapshot_date": dt.date.today().isoformat(),
                        "country": country,
                        "category": category,
                        "rank": i,
                        "app_id": app.get("id"),
                        "name": app.get("name"),
                        "developer": app.get("artistName"),
                        "app_store_url": app.get("url"),
                        "icon_url": app.get("artworkUrl100"),
                    })
                return pd.DataFrame(rows)
            else:
                last_exc = RuntimeError(f"HTTP {r.status_code} for {url}")
        except Exception as e:
            last_exc = e
        time.sleep(SLEEP_BETWEEN_RETRIES)
    raise last_exc if last_exc else RuntimeError(f"Fetch failed for {url}")

def ensure_sqlite_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            snapshot_date TEXT,
            country TEXT,
            category TEXT,
            rank INTEGER,
            app_id TEXT,
            name TEXT,
            developer TEXT,
            app_store_url TEXT,
            icon_url TEXT,
            change TEXT
        )
    """)
    conn.commit()

def insert_into_db(conn: sqlite3.Connection, df: pd.DataFrame):
    df.to_sql("positions", conn, if_exists="append", index=False)

def latest_previous_folder(today: str) -> Path | None:
    if not DATA_DIR.exists():
        return None
    dates = []
    for p in DATA_DIR.iterdir():
        if p.is_dir() and p.name < today:
            dates.append(p.name)
    if not dates:
        return None
    dates.sort()
    return DATA_DIR / dates[-1]

def compute_change(df_now: pd.DataFrame, df_prev: pd.DataFrame) -> pd.DataFrame:
    # Сравняваме по ключ (country, category, app_id)
    key = ["country", "category", "app_id"]

    prev_min = df_prev[key + ["rank"]].rename(columns={"rank": "prev_rank"})
    merged = df_now.merge(prev_min, on=key, how="left")

    def fmt_change(row):
        if pd.isna(row.get("prev_rank")):
            return "NEW"
        diff = int(row["prev_rank"]) - int(row["rank"])
        if diff > 0:
            return f"+{diff}"
        elif diff < 0:
            return f"{diff}"
        else:
            return "0"

    merged["change"] = merged.apply(fmt_change, axis=1)

    # Открити "OUT" (били преди, няма ги сега)
    now_keys = set(tuple(x) for x in df_now[key].to_numpy())
    outs = []
    for _, row in df_prev.iterrows():
        k = (row["country"], row["category"], row["app_id"])
        if k not in now_keys:
            outs.append({
                "snapshot_date": df_now["snapshot_date"].iloc[0],
                "country": row["country"],
                "category": row["category"],
                "rank": None,
                "app_id": row["app_id"],
                "name": row["name"],
                "developer": row["developer"],
                "app_store_url": row["app_store_url"],
                "icon_url": row["icon_url"],
                "change": "OUT"
            })
    if outs:
        merged = pd.concat([merged, pd.DataFrame(outs)], ignore_index=True)

    # Подредба: първо по държава/категория, после по rank (None отива надолу)
    merged["rank_sort"] = merged["rank"].fillna(9999)
    merged = merged.sort_values(["country", "category", "rank_sort", "name"]).drop(columns=["rank_sort"])
    return merged

def main():
    today = dt.date.today().isoformat()
    out_dir = DATA_DIR / today
    out_dir.mkdir(parents=True, exist_ok=True)

    frames: List[pd.DataFrame] = []
    for c in COUNTRIES:
        for cat in CATEGORIES:
            try:
                df = fetch_one(c, cat)
                frames.append(df)
                print(f"[OK] {c} / {cat} ({len(df)} rows)")
            except Exception as e:
                print(f"[WARN] fetch failed for {c}/{cat}: {e}", file=sys.stderr)

    if not frames:
        print("No data fetched. Exiting.", file=sys.stderr)
        sys.exit(1)

    df_now = pd.concat(frames, ignore_index=True)
    csv_now = out_dir / "top_free_all.csv"
    df_now.to_csv(csv_now, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")
    print(f"Saved: {csv_now}")

    # Предишна папка (ако има)
    prev_dir = latest_previous_folder(today)
    if prev_dir and (prev_dir / "top_free_all.csv").exists():
        df_prev = pd.read_csv(prev_dir / "top_free_all.csv", dtype=str)
        # ensure numeric rank in prev
        df_prev["rank"] = pd.to_numeric(df_prev["rank"], errors="coerce")
        df_now["rank"] = pd.to_numeric(df_now["rank"], errors="coerce")

        df_out = compute_change(df_now, df_prev)
        csv_change = out_dir / "top_free_all_with_change.csv"
        df_out.to_csv(csv_change, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")
        print(f"Saved: {csv_change}")
    else:
        # първи рън – просто копираме df_now и слагаме change="NEW"
        df_first = df_now.copy()
        df_first["change"] = "NEW"
        csv_change = out_dir / "top_free_all_with_change.csv"
        df_first.to_csv(csv_change, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8")
        print(f"Saved: {csv_change} (first run)")

    # Запис в SQLite
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_sqlite_schema(conn)
        df_to_db = pd.read_csv(out_dir / "top_free_all_with_change.csv", dtype=str)
        insert_into_db(conn, df_to_db)
        print(f"SQLite updated: {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
