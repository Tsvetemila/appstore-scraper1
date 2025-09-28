import json
import os
import pathlib
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests
from dateutil import tz
from sqlalchemy import (
    create_engine, text
)

# ----------------------------
# Конфигурация
# ----------------------------
# Държави (ISO2)
COUNTRIES = ["US", "GB", "DE", "FR", "IT", "ES"]

# Категории -> genre id от App Store (RSS v1)
CATEGORIES: Dict[str, Optional[int]] = {
    "Overall": None,       # общ топ без жанр
    "Games": 6014,
    "Music": 6011,
    "Social Networking": 6005,
    "Productivity": 6007,
    "Photo & Video": 6008,
    "Entertainment": 6016,
    "News": 6009,
}

CHART_TYPE = "TopFree"  # дърпаме само Top Free за стабилност

# Къде да пишем CSV
OUT_DIR = pathlib.Path("out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# SQLite база в корена
DB_PATH = pathlib.Path("appstore_charts.db")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", future=True)


# ----------------------------
# Помощни
# ----------------------------
def rss_url(country: str, genre_id: Optional[int]) -> str:
    """
    iTunes RSS v1:
    overall: https://itunes.apple.com/us/rss/topfreeapplications/limit=50/json
    category: https://itunes.apple.com/us/rss/topfreeapplications/limit=50/genre=6014/json
    """
    base = f"https://itunes.apple.com/{country.lower()}/rss/topfreeapplications/limit=50"
    if genre_id is not None:
        base += f"/genre={genre_id}"
    return base + "/json"


def fetch_feed(country: str, category: str, genre_id: Optional[int]) -> List[dict]:
    url = rss_url(country, genre_id)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    entries = data.get("feed", {}).get("entry", [])
    # Ако няма записи, връщаме празно
    return entries if isinstance(entries, list) else []


def normalize_entry(entry: dict, rank: int, country: str, category: str) -> dict:
    # безопасни гетъри
    def g(d, *path, default=None):
        for p in path:
            if isinstance(d, dict) and p in d:
                d = d[p]
            else:
                return default
        return d

    app_id = g(entry, "id", "attributes", "im:id", default=None)
    app_store_url = g(entry, "id", "label", default=None)
    name = g(entry, "im:name", "label", default=None)
    developer = g(entry, "im:artist", "label", default=None)
    developer_url = g(entry, "im:artist", "attributes", "href", default=None)

    # иконата — взимаме най-голямата
    images = entry.get("im:image", [])
    icon_url = images[-1]["label"] if images else None

    # genre/категория от entry (може да е различно от избраното)
    entry_category = g(entry, "category", "attributes", "term", default=category)
    genre_id = g(entry, "category", "attributes", "im:id", default=None)

    return {
        "app_id": app_id,
        "name": name,
        "developer": developer,
        "developer_url": developer_url,
        "icon_url": icon_url,
        "app_store_url": app_store_url,
        "entry_category": entry_category,
        "entry_genre_id": genre_id,
        "rank": rank,
        "country": country,
        "category": category,
    }


def ensure_schema():
    with ENGINE.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT NOT NULL,
            category TEXT NOT NULL,
            chart_type TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            UNIQUE(country, category, chart_type, created_at_utc)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS apps (
            app_id TEXT PRIMARY KEY,
            name TEXT,
            developer TEXT,
            developer_url TEXT,
            app_store_url TEXT,
            icon_url TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ranks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            app_id TEXT NOT NULL,
            rank INTEGER NOT NULL,
            country TEXT NOT NULL,
            category TEXT NOT NULL,
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(id),
            FOREIGN KEY(app_id) REFERENCES apps(app_id)
        );
        """))


def insert_snapshot(country: str, category: str, chart_type: str) -> int:
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with ENGINE.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO snapshots (country, category, chart_type, created_at_utc)
            VALUES (:country, :category, :chart_type, :ts)
            """),
            {"country": country, "category": category, "chart_type": chart_type, "ts": ts}
        )
        snap_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()
    return snap_id


def upsert_apps(rows: List[dict]):
    if not rows:
        return
    with ENGINE.begin() as conn:
        for r in rows:
            conn.execute(
                text("""
                INSERT INTO apps (app_id, name, developer, developer_url, app_store_url, icon_url)
                VALUES (:app_id, :name, :developer, :developer_url, :app_store_url, :icon_url)
                ON CONFLICT(app_id) DO UPDATE SET
                    name=excluded.name,
                    developer=excluded.developer,
                    developer_url=excluded.developer_url,
                    app_store_url=excluded.app_store_url,
                    icon_url=excluded.icon_url;
                """),
                {
                    "app_id": r["app_id"],
                    "name": r["name"],
                    "developer": r["developer"],
                    "developer_url": r["developer_url"],
                    "app_store_url": r["app_store_url"],
                    "icon_url": r["icon_url"],
                }
            )


def insert_ranks(snapshot_id: int, rows: List[dict], country: str, category: str):
    if not rows:
        return
    with ENGINE.begin() as conn:
        for r in rows:
            conn.execute(
                text("""
                INSERT INTO ranks (snapshot_id, app_id, rank, country, category)
                VALUES (:snapshot_id, :app_id, :rank, :country, :category)
                """),
                {
                    "snapshot_id": snapshot_id,
                    "app_id": r["app_id"],
                    "rank": r["rank"],
                    "country": country,
                    "category": category,
                }
            )


def write_csv(rows: List[dict], country: str, category: str):
    if not rows:
        return
    today = datetime.now(tz=tz.UTC).date().isoformat()
    fn = OUT_DIR / f"top50_{country}_{category.replace(' ', '_')}_{today}.csv"
    df = pd.DataFrame(rows)
    # подреждаме колоните четимо
    cols = [
        "rank", "app_id", "name", "developer", "country", "category",
        "entry_category", "entry_genre_id",
        "app_store_url", "icon_url", "developer_url"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df.to_csv(fn, index=False, encoding="utf-8")
    print(f"[CSV] {fn}")


def run_all():
    ensure_schema()

    for country in COUNTRIES:
        for category, genre_id in CATEGORIES.items():
            try:
                entries = fetch_feed(country, category, genre_id)
                rows = [
                    normalize_entry(e, idx + 1, country, category)
                    for idx, e in enumerate(entries)
                    if isinstance(e, dict)
                ]
                if not rows:
                    print(f"[WARN] Empty feed: {country} / {category}")
                    continue

                # 1) snapshot
                snap_id = insert_snapshot(country, category, CHART_TYPE)

                # 2) upsert apps
                upsert_apps(rows)

                # 3) ranks
                insert_ranks(snap_id, rows, country, category)

                # 4) csv
                write_csv(rows, country, category)

                print(f"[OK] {country} / {category} (TopFree) rows={len(rows)} snap_id={snap_id}")

            except Exception as ex:
                print(f"[ERR] {country}/{category}: {ex}")


if __name__ == "__main__":
    run_all()
