# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import pandas as pd
from typing import List
from datetime import datetime
from fastapi.responses import StreamingResponse
import io

app = FastAPI()

# Allow frontend (Vite/React) to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "appstore_charts.db"

# --------- Helper Functions ---------
def get_latest_snapshot(country: str, category: str):
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT * FROM charts
        WHERE country = '{country}' AND category = '{category}'
        ORDER BY snapshot_date DESC, rank ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return None
    latest_date = df["snapshot_date"].iloc[0]
    return df[df["snapshot_date"] == latest_date]

def get_previous_snapshot(country: str, category: str):
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT * FROM charts
        WHERE country = '{country}' AND category = '{category}'
        ORDER BY snapshot_date DESC, rank ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty or len(df["snapshot_date"].unique()) < 2:
        return None
    latest_date = df["snapshot_date"].unique()[0]
    prev_date = df["snapshot_date"].unique()[1]
    return df[df["snapshot_date"] == prev_date]

def get_last_n_snapshots(country: str, category: str, n: int = 7):
    conn = sqlite3.connect(DB_PATH)
    query = f"""
        SELECT * FROM charts
        WHERE country = '{country}' AND category = '{category}'
        ORDER BY snapshot_date DESC, rank ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        return None
    unique_dates = df["snapshot_date"].unique()[:n]
    history = {d: df[df["snapshot_date"] == d] for d in unique_dates}
    return history

def compare_snapshots(current: pd.DataFrame, previous: pd.DataFrame):
    current = current.copy()
    previous = previous.copy()

    changes = []
    for _, row in current.iterrows():
        app_id = row["app_id"]
        prev_row = previous[previous["app_id"] == app_id]

        if not prev_row.empty:
            prev_rank = int(prev_row["rank"].values[0])
            delta = prev_rank - int(row["rank"])
            change = "↑" if delta > 0 else "↓" if delta < 0 else "="
        else:
            change = "🆕"
        changes.append(change)

    current["change"] = changes

    # Добавяме и отпадналите
    dropped = []
    for _, row in previous.iterrows():
        if row["app_id"] not in current["app_id"].values:
            dropped.append({
                "snapshot_date": current["snapshot_date"].iloc[0],
                "country": row["country"],
                "category": row["category"],
                "rank": "-",
                "app_id": row["app_id"],
                "name": row["name"],
                "developer": row["developer"],
                "url": row["url"],
                "artwork": row["artwork"],
                "change": "❌ Dropped"
            })
    if dropped:
        current = pd.concat([current, pd.DataFrame(dropped)], ignore_index=True)

    return current

def compare_with_history(current: pd.DataFrame, history: dict):
    current = current.copy()
    all_history = pd.concat(history.values(), ignore_index=True)

    changes = []
    for _, row in current.iterrows():
        app_id = row["app_id"]

        # Проверяваме последния snapshot (вчера или преди дни)
        sorted_dates = sorted(history.keys(), reverse=True)
        previous_day = None
        if len(sorted_dates) > 1:
            previous_day = history[sorted_dates[1]]

        prev_row = None
        if previous_day is not None:
            prev_row = previous_day[previous_day["app_id"] == app_id]

        if prev_row is not None and not prev_row.empty:
            # Бил е и вчера → mover
            prev_rank = int(prev_row["rank"].values[0])
            delta = prev_rank - int(row["rank"])
            change = "↑" if delta > 0 else "↓" if delta < 0 else "="
        else:
            # Не е бил вчера, проверяваме в историята (7 дни)
            in_history = app_id in all_history["app_id"].values
            if in_history:
                change = "RE-ENTRY"
            else:
                change = "NEW"

        changes.append(change)

    current["change"] = changes

    # Отпаднали приложения (били са в историята, но ги няма сега)
    dropped = []
    latest_date = current["snapshot_date"].iloc[0]
    last_week_apps = all_history["app_id"].unique()

    for _, row in all_history.iterrows():
        if row["app_id"] not in current["app_id"].values and row["app_id"] in last_week_apps:
            dropped.append({
                "snapshot_date": latest_date,
                "country": row["country"],
                "category": row["category"],
                "rank": "-",
                "app_id": row["app_id"],
                "name": row["name"],
                "developer": row["developer"],
                "url": row["url"],
                "artwork": row["artwork"],
                "change": "❌ Dropped"
            })

    if dropped:
        current = pd.concat([current, pd.DataFrame(dropped)], ignore_index=True)

    return current

# --------- API Endpoints ---------
@app.get("/latest")
def latest(country: str, category: str):
    df = get_latest_snapshot(country, category)
    if df is None:
        raise HTTPException(status_code=404, detail="No data found")
    return df.to_dict(orient="records")

@app.get("/compare")
def compare(country: str, category: str):
    current = get_latest_snapshot(country, category)
    previous = get_previous_snapshot(country, category)

    if current is None:
        raise HTTPException(status_code=404, detail="No data found")
    if previous is None:
        return {"detail": "Not enough data for comparison"}

    compared = compare_snapshots(current, previous)
    return compared.to_dict(orient="records")

@app.get("/compare7")
def compare7(country: str, category: str):
    history = get_last_n_snapshots(country, category, n=7)
    if history is None or len(history) < 2:
        raise HTTPException(status_code=404, detail="Not enough data for 7-day comparison")

    latest_date = sorted(history.keys(), reverse=True)[0]
    current = history[latest_date]

    compared = compare_with_history(current, history)
    return compared.to_dict(orient="records")

@app.get("/export_csv")
def export_csv(country: str, category: str, compare: bool = False, compare7: bool = False):
    if compare7:
        history = get_last_n_snapshots(country, category, n=7)
        if history is None or len(history) < 2:
            raise HTTPException(status_code=404, detail="Not enough data for 7-day comparison")
        latest_date = sorted(history.keys(), reverse=True)[0]
        current = history[latest_date]
        df = compare_with_history(current, history)
    elif compare:
        current = get_latest_snapshot(country, category)
        previous = get_previous_snapshot(country, category)
        if current is None or previous is None:
            raise HTTPException(status_code=404, detail="Not enough data")
        df = compare_snapshots(current, previous)
    else:
        df = get_latest_snapshot(country, category)
        if df is None:
            raise HTTPException(status_code=404, detail="No data")

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    filename = f"AppStore_{country}_{category}_{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
