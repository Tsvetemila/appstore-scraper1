import os
import csv
import json
import sqlite3
import requests
import datetime as dt
from pathlib import Path

# --- Настройки ---
EXPORT_DIR = Path("exports")
DB_PATH = Path("appstore_charts.db")

EXPORT_DIR.mkdir(exist_ok=True)

# Държави според брифа
COUNTRIES = [
    "US", "GB", "DE", "FR", "IT", "ES", "SE", "NL", "JP"
]

# Основни категории
CATEGORIES = {
    "Overall": 36,
    "Education": 6017,
    "Games": 6014,
    "Music": 6011,
    "Productivity": 6007,
    "Social Networking": 6005,
    "Photo & Video": 6008
}

# Подкатегории на игри (примерен сет)
GAME_GENRES = {
    "Action": 7001,
    "Adventure": 7002,
    "Arcade": 7003,
    "Board": 7004,
    "Card": 7005,
    "Casino": 7006,
    "Casual": 7007,
    "Family": 7009,
    "Music": 7011,
    "Puzzle": 7012,
    "Racing": 7013,
    "Role Playing": 7014,
    "Simulation": 7015,
    "Sports": 7016,
    "Strategy": 7017,
    "Trivia": 7018,
    "Word": 7019
}

# Chart типове
CHARTS = ["top-free", "top-paid"]

# --- DB setup ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS apps (
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
        developer_id TEXT,
        app_store_url TEXT,
        icon_url TEXT
    )
    """)
    conn.commit()
    conn.close()

# --- Запазване в DB ---
def save_to_db(rows):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany("""
    INSERT INTO apps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

# --- Сваляне на данни от RSS ---
def fetch_rss(country, chart, genre_id=None):
    url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/{chart}/50/apps.json"
    if genre_id:
        url = f"https://rss.applemarketingtools.com/api/v2/{country.lower()}/apps/{chart}/50/apps.json?genre={genre_id}"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        print(f"⚠️ Failed fetch {country} {chart} genre={genre_id}")
        return []
    return resp.json().get("feed", {}).get("results", [])

# --- Главна функция ---
def run_scraper():
    snapshot_date = dt.date.today().isoformat()
    all_rows = []

    for country in COUNTRIES:
        for chart in CHARTS:
            # Основни категории
            for cat, genre_id in CATEGORIES.items():
                apps = fetch_rss(country, chart, genre_id)
                for rank, app in enumerate(apps, start=1):
                    all_rows.append((
                        snapshot_date,
                        country,
                        chart,
                        cat,
                        genre_id,
                        rank,
                        app.get("id"),
                        app.get("bundleId"),
                        app.get("name"),
                        app.get("artistName"),
                        app.get("artistId"),
                        app.get("url"),
                        app.get("artworkUrl100")
                    ))

            # Подкатегории за игри
            for subcat, genre_id in GAME_GENRES.items():
                apps = fetch_rss(country, chart, genre_id)
                for rank, app in enumerate(apps, start=1):
                    all_rows.append((
                        snapshot_date,
                        country,
                        chart,
                        f"Games - {subcat}",
                        genre_id,
                        rank,
                        app.get("id"),
                        app.get("bundleId"),
                        app.get("name"),
                        app.get("artistName"),
                        app.get("artistId"),
                        app.get("url"),
                        app.get("artworkUrl100")
                    ))

    # --- Запис в база ---
    save_to_db(all_rows)

    # --- Запис в CSV ---
    csv_file = EXPORT_DIR / f"apps_top50_export_{snapshot_date}.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "snapshot_date", "country", "chart", "category", "genre_id",
            "rank", "app_id", "bundle_id", "name", "developer",
            "developer_id", "app_store_url", "icon_url"
        ])
        writer.writerows(all_rows)

    print(f"✅ Done: {len(all_rows)} rows saved | {csv_file}")


if __name__ == "__main__":
    init_db()
    run_scraper()
