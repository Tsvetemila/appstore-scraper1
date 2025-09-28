import os
import csv
import sqlite3
import argparse
from pathlib import Path

def init_db(db_path: Path):
    conn = sqlite3.connect(db_path)
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

def ingest_csv_to_db(csv_dir: Path, db_path: Path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    csv_files = list(csv_dir.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ Няма CSV файлове за импорт в {csv_dir}")
        return

    for csv_file in csv_files:
        print(f"📥 Импортирам: {csv_file.name}")
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                # Уверяваме се, че всички колони съществуват
                rows.append((
                    row.get("snapshot_date"),
                    row.get("country"),
                    row.get("chart"),
                    row.get("category"),
                    row.get("genre_id"),
                    row.get("rank"),
                    row.get("app_id"),
                    row.get("bundle_id"),
                    row.get("name"),
                    row.get("developer"),
                    row.get("developer_id"),
                    row.get("app_store_url"),
                    row.get("icon_url"),
                ))
            if rows:
                cur.executemany("""
                INSERT INTO apps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                print(f"✅ {len(rows)} реда добавени от {csv_file.name}")
            else:
                print(f"⚠️ Празен CSV: {csv_file.name}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=str, required=True, help="Папка с CSV файлове")
    parser.add_argument("--db", type=str, required=True, help="Път до SQLite DB")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    db_path = Path(args.db)

    init_db(db_path)
    ingest_csv_to_db(csv_dir, db_path)
