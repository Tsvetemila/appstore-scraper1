# scraper/ingest_to_sqlite.py
import csv
import re
import sqlite3
from pathlib import Path
from datetime import datetime
import argparse

def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS charts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_date TEXT NOT NULL,          -- YYYY-MM-DD (денят на скрейпа)
        snapshot_ts   TEXT,                   -- ISO timestamp от CSV, ако има
        country       TEXT NOT NULL,
        category      TEXT NOT NULL,          -- голяма категория (Overall, Games, Music, …)
        subcategory   TEXT,                   -- подкатегория (Puzzle, Action … ако има)
        chart         TEXT NOT NULL,          -- Top Free (за сега фиксираме)
        rank          INTEGER,
        previous_rank INTEGER,
        delta         INTEGER,
        status        TEXT,                   -- IN_TOP / NEW / RE-ENTRY / OUT и пр., ако има
        app_id        TEXT,                   -- numeric app id
        bundle_id     TEXT,                   -- bundle id ако е наличен
        name          TEXT,                   -- име на приложението
        developer     TEXT,
        app_store_url TEXT,
        icon_url      TEXT,
        source        TEXT,                   -- AppleRSSMarketing / iTunesRSS и пр., ако има
        UNIQUE(snapshot_date, country, category, chart, app_id)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_charts_date ON charts(snapshot_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_charts_key  ON charts(country, category, chart)")
    conn.commit()

def _to_int(v):
    if v is None:
        return None
    v = str(v).strip()
    if v == "" or v.lower() == "null":
        return None
    try:
        return int(v)
    except ValueError:
        # понякога идва "12↑" или подобни – чистим всичко, освен цифри и минус
        v2 = re.sub(r"[^0-9\-]", "", v)
        return int(v2) if v2 not in ("", "-", "--") else None

# мапваме различни възможни заглавия на колони към нашите ключове
HEADER_MAP = {
    "app": "name",
    "app_name": "name",
    "title": "name",

    "developer_name": "developer",
    "developer": "developer",

    "category": "category",
    "primary_genre": "category",

    "subcategory": "subcategory",
    "sub_category": "subcategory",

    "country": "country",
    "storefront": "country",

    "rank": "rank",
    "current_rank": "rank",

    "previous_rank": "previous_rank",

    "delta": "delta",
    "change": "delta",

    "status": "status",

    "app_id": "app_id",
    "id": "app_id",

    "bundle_id": "bundle_id",

    "url": "app_store_url",
    "app_store_url": "app_store_url",

    "icon_url": "icon_url",

    "snapshot_timestamp": "snapshot_ts",
    "timestamp": "snapshot_ts",
    "snapshot_week": "source",  # ако в CSV имаш week/source – пазим го в source
    "source": "source",

    "chart": "chart"
}

def normalize_row(raw: dict):
    """ Връща речник с ключове по нашата схема, независимо от CSV заглавията. """
    out = {k: None for k in [
        "name","developer","category","subcategory","country",
        "rank","previous_rank","delta","status","app_id","bundle_id",
        "app_store_url","icon_url","snapshot_ts","source","chart"
    ]}
    # нормализирай всички ключове към lower
    lr = { (k or "").strip().lower(): v for k, v in raw.items() }

    for k, v in lr.items():
        target = HEADER_MAP.get(k)
        if target:
            out[target] = v

    # числови полета
    out["rank"] = _to_int(out["rank"])
    out["previous_rank"] = _to_int(out["previous_rank"])
    out["delta"] = _to_int(out["delta"])

    # chart – ако го няма в CSV, фиксираме "Top Free" (по текущата ни логика)
    if not out["chart"] or str(out["chart"]).strip() == "":
        out["chart"] = "Top Free"

    return out

def infer_snapshot_date_from_filename(p: Path) -> str:
    # търсим YYYY-MM-DD в името на файла
    m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
    if m:
        return m.group(1)
    # fallback – днес
    return datetime.utcnow().strftime("%Y-%m-%d")

def ingest_dir(csv_dir: Path, db_path: Path):
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)

    csv_files = sorted(csv_dir.glob("*.csv"))
    total = 0

    for f in csv_files:
        snapshot_date = infer_snapshot_date_from_filename(f)
        with f.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                row = normalize_row(raw)

                conn.execute("""
                INSERT OR IGNORE INTO charts
                (snapshot_date, snapshot_ts, country, category, subcategory, chart,
                 rank, previous_rank, delta, status, app_id, bundle_id, name,
                 developer, app_store_url, icon_url, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    snapshot_date,
                    row["snapshot_ts"],
                    (row["country"] or "").upper(),
                    row["category"] or "Overall",
                    row["subcategory"],
                    row["chart"],
                    row["rank"],
                    row["previous_rank"],
                    row["delta"],
                    row["status"],
                    row["app_id"],
                    row["bundle_id"],
                    row["name"],
                    row["developer"],
                    row["app_store_url"],
                    row["icon_url"],
                    row["source"],
                ))
                total += 1
        conn.commit()

    print(f"[ingest] Imported rows: {total} from {len(csv_files)} csv files into {db_path}")
    conn.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-dir", default="out", help="Папка с CSV файловете")
    ap.add_argument("--db", default="appstore_charts.db", help="SQLite база, която да създадем/обновим")
    args = ap.parse_args()

    csv_dir = Path(args.csv_dir)
    db_path = Path(args.db)
    if not csv_dir.exists():
        raise SystemExit(f"[ingest] CSV directory not found: {csv_dir.resolve()}")

    ingest_dir(csv_dir, db_path)

if __name__ == "__main__":
    main()
