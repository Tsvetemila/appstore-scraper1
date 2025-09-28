import sqlite3
import pandas as pd
from pathlib import Path
import datetime as dt

# Път до CSV файловете
CSV_DIR = Path("output")   # това е папката, където GitHub Actions пази CSV-то
DB_PATH = Path("appstore_charts.db")

def ingest_latest_csv():
    # Намираме последния CSV по време
    csv_files = sorted(CSV_DIR.glob("*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
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
    conn = sqlite3.connect(DB_PATH)

    # Записваме в таблица apps (append = добавя нови записи)
    df.to_sql("apps", conn, if_exists="append", index=False)

    conn.close()
    print(f"✅ Импортирах {len(df)} реда в {DB_PATH}")

if __name__ == "__main__":
    ingest_latest_csv()
