# scraper/scraper.py
import os
import csv
import json
import time
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional

import requests


# ---------------------------
# Конфигурация (редактирай спокойно)
# ---------------------------

# Държави (ISO 2 букви). Допълвай/редактирай според брифа.
COUNTRIES = {
    "US": "United States",
    "GB": "United Kingdom",
    "JP": "Japan",
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "SE": "Sweden",
}

# Категории (основни) по жанр-ID в App Store.
# Overall = None (без жанр), останалите са с genreId.
TOP_CATEGORIES = {
    "Overall": None,
    "Games": 6014,
    "Music": 6011,
    "Productivity": 6007,
    "Photo & Video": 6008,
    "Social Networking": 6005,
    "Shopping": 6024,
    "Finance": 6015,
    "Health & Fitness": 6013,
    "News": 6009,
    "Sports": 6004,
    "Travel": 6003,
    "Food & Drink": 6023,
    "Education": 6017,
}

# Подкатегории (subgenres) на Games
GAME_SUBGENRES = {
    7001: "Action", 7002: "Adventure", 7003: "Arcade", 7004: "Board",
    7005: "Card", 7006: "Casino", 7007: "Dice", 7008: "Educational",
    7009: "Family", 7011: "Music", 7012: "Puzzle", 7013: "Racing",
    7014: "Role Playing", 7015: "Simulation", 7016: "Sports",
    7017: "Strategy", 7018: "Trivia", 7019: "Word"
}

# Ограничение (топ 50)
LIMIT = 50

# Папки за данни
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
EXPORT_DIR = ROOT / "exports"
DB_PATH = DATA_DIR / "appstore_charts.db"

DATA_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# Полетата, които пазим (CSV и SQLite)
CSV_HEADERS = [
    "snapshot_date", "country", "chart", "category", "genre_id",
    "rank", "app_id", "bundle_id", "name", "developer",
    "app_store_url", "icon_url"
]

# ---------------------------
# Помощни – HTTP
# ---------------------------

def http_get_json(url: str, timeout: int = 30) -> Optional[dict]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def fetch_marketing_v2(country: str, genre_id: Optional[int], limit: int = LIMIT) -> Optional[List[dict]]:
    """
    Нов RSS (Apple Marketing Tools). Връща list от обекти (или None при проблем).
    Примери:
    - overall: https://rss.applemarketingtools.com/api/v2/us/apps/top-free/50/apps.json
    - by genre: https://rss.applemarketingtools.com/api/v2/us/apps/top-free/6014/50/apps.json
    """
    base = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/top-free/"
    if genre_id:
        url = f"{base}{genre_id}/{limit}/apps.json"
    else:
        url = f"{base}{limit}/apps.json"

    data = http_get_json(url)
    if not data or "feed" not in data or "results" not in data["feed"]:
        return None
    return data["feed"]["results"]


def fetch_itunes_legacy(country: str, genre_id: Optional[int], limit: int = LIMIT) -> Optional[List[dict]]:
    """
    Стар RSS (iTunes). Връща list от entries (или None).
    Примери:
    - overall: https://itunes.apple.com/us/rss/topfreeapplications/limit=50/json
    - by genre: https://itunes.apple.com/us/rss/topfreeapplications/limit=50/genre=6014/json
    """
    if genre_id:
        url = f"https://itunes.apple.com/{country.lower()}/rss/topfreeapplications/limit={limit}/genre={genre_id}/json"
    else:
        url = f"https://itunes.apple.com/{country.lower()}/rss/topfreeapplications/limit={limit}/json"

    data = http_get_json(url)
    if not data or "feed" not in data:
        return None
    return data["feed"].get("entry") or []


# ---------------------------
# Парсване към унифициран вид
# ---------------------------

def normalize_marketing_v2(items: List[dict]) -> List[dict]:
    """
    Нормализира резултат от marketingtools към общ формат (виж CSV_HEADERS).
    """
    out = []
    for i, it in enumerate(items, start=1):
        out.append({
            "rank": i,
            "app_id": it.get("id"),
            "bundle_id": it.get("bundleId"),
            "name": it.get("name"),
            "developer": it.get("artistName"),
            "app_store_url": it.get("url"),
            "icon_url": it.get("artworkUrl100"),
        })
    return out


def normalize_itunes_legacy(items: List[dict]) -> List[dict]:
    """
    Нормализира стария iTunes RSS формат към общ формат.
    """
    out = []
    for i, it in enumerate(items, start=1):
        # iTunes RSS е по-различен по ключове (im:name, im:image, id, etc.)
        app_id = None
        try:
            app_id = it["id"]["attributes"]["im:id"]
        except Exception:
            pass

        name = it.get("im:name", {}).get("label")
        developer = it.get("im:artist", {}).get("label")
        app_url = it.get("id", {}).get("label")
        # взимаме най-голямата икона
        images = it.get("im:image", [])
        icon_url = images[-1]["label"] if images else None

        out.append({
            "rank": i,
            "app_id": app_id,
            "bundle_id": None,      # не винаги го има в този feed
            "name": name,
            "developer": developer,
            "app_store_url": app_url,
            "icon_url": icon_url,
        })
    return out


def fetch_one_chart(country: str, category_name: str, genre_id: Optional[int]) -> List[dict]:
    """
    Връща списък от нормализирани приложения (Top Free 1..50) за дадена държава/категория/жанр.
    Пробва първо новия RSS; ако не стане – пада към стария.
    """
    # 1) Новият RSS
    v2 = fetch_marketing_v2(country, genre_id, limit=LIMIT)
    if v2:
        return normalize_marketing_v2(v2)

    # 2) Старият iTunes RSS
    itunes = fetch_itunes_legacy(country, genre_id, limit=LIMIT)
    if itunes:
        return normalize_itunes_legacy(itunes)

    return []


# ---------------------------
# SQLite
# ---------------------------

def ensure_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chart_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT,
            country TEXT,
            chart TEXT,
            category TEXT,
            genre_id INTEGER,
            rank INTEGER,
            app_id TEXT,
            bundle_id TEXT,
            name TEXT,
            developer TEXT,
            app_store_url TEXT,
            icon_url TEXT
        )
    """)
    con.commit()
    con.close()


def save_rows_sqlite(rows: List[dict]):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executemany(f"""
        INSERT INTO chart_items
        ({", ".join(CSV_HEADERS)})
        VALUES ({", ".join(["?"] * len(CSV_HEADERS))})
    """, [[r[h] for h in CSV_HEADERS] for r in rows])
    con.commit()
    con.close()


def save_csv(rows: List[dict], country: str, category_name: str, genre_id: Optional[int], snapshot_date: str):
    country_code = country.upper()
    cat_slug = category_name.lower().replace("&", "and").replace(" ", "_")
    suffix = f"_{genre_id}" if genre_id else ""
    fname = f"apps_top50_{country_code}_{cat_slug}{suffix}_{snapshot_date}.csv"
    path = EXPORT_DIR / fname
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"[CSV] saved → {path}")


# ---------------------------
# Основна логика
# ---------------------------

def run():
    ensure_db()
    snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Списък от всичко за минаване:
    #  - всички основни категории
    #  - ако категорията е Games, първо взимаме общите Games (6014),
    #    после всяка подкатегория (GAME_SUBGENRES)
    tasks = []
    for ccode, _ in COUNTRIES.items():
        for cat_name, cat_genre in TOP_CATEGORIES.items():
            if cat_name == "Games":
                # 1) самите Games (6014)
                tasks.append((ccode, cat_name, cat_genre))
                # 2) подкатегории
                for sub_id in GAME_SUBGENRES.keys():
                    tasks.append((ccode, f"Games - {GAME_SUBGENRES[sub_id]}", sub_id))
            else:
                tasks.append((ccode, cat_name, cat_genre))

    total = len(tasks)
    print(f"Plan: {total} tasks (countries x categories).")

    for idx, (country, cat_name, genre_id) in enumerate(tasks, start=1):
        pct = int(idx / total * 100)
        print(f"[{idx}/{total}] {pct}% → {country} / {cat_name} (genre={genre_id}) ...", flush=True)

        items = fetch_one_chart(country, cat_name, genre_id)
        if not items:
            print(f"  ⚠️  EMPTY (no data) → {country} / {cat_name}")
            continue

        # Обогатяваме всеки ред с метаданни (snapshot и т.н.)
        rows = []
        for it in items:
            row = {
                "snapshot_date": snapshot_date,
                "country": country,
                "chart": "Top Free",
                "category": cat_name,
                "genre_id": genre_id if genre_id else None,
                "rank": it.get("rank"),
                "app_id": it.get("app_id"),
                "bundle_id": it.get("bundle_id"),
                "name": it.get("name"),
                "developer": it.get("developer"),
                "app_store_url": it.get("app_store_url"),
                "icon_url": it.get("icon_url"),
            }
            rows.append(row)

        # Пишем в SQLite
        save_rows_sqlite(rows)
        # и CSV
        save_csv(rows, country, cat_name, genre_id, snapshot_date)

        # Малка пауза за уважение към Apple
        time.sleep(0.7)


if __name__ == "__main__":
    run()
