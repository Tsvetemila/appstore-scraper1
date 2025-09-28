import sqlite3
import pandas as pd
from pathlib import Path
import datetime as dt
import argparse


def ingest_csv(csv_dir: Path, db_path: Path):
    # Намираме последния CSV по време
    csv_files = sorted(Path(csv_dir).glob("*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not csv_files:
        print("❌ Няма CSV файлове за импортиране")
        return

    latest_csv = csv_files[0]
    print(f"➡ Импортирам: {latest_csv}")

    # Зареждаме CSV-то
    df = pd.read_csv(latest_csv)

    # Добавяме колоната snapshot_date ако я няма
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = dt.date.today().isoformat()

    # Връзка с базата
    conn = sqlite3.connect(db_path)

    # Записваме в таблица apps (append = добавя нови записи)
    df.to_sql("apps", conn, if_exists="append", index=False)

    conn.close()
    print(f"✅ Импортирах {len(df)} реда в {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=str, default="exports", help="Папка с CSV файлове")
    parser.add_argument("--db", type=str, default="appstore_charts.db", help="Име на SQLite база")
    args = parser.parse_args()

    ingest_csv(Path(args.csv_dir), Path(args.db))
