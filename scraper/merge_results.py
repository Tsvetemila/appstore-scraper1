# merge_results.py
import sqlite3, os, csv
from pathlib import Path
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

def export_latest_csv(db_path: str, out_dir: str | Path | None = None) -> Path:
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
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"[OK] exported combined CSV -> {out_path}")
    return out_path

if __name__ == "__main__":
    export_latest_csv(DB_PATH)
