# ------------------------- X) Weekly Insights (New & Re-Entry for last week) ---------------
@app.get("/weekly/insights")
def weekly_insights(
    country: str = "US",
    category: Optional[str] = Query(None),
    subcategory: Optional[str] = Query(None),
    lookback_days: int = 7,
    status: Optional[str] = Query(None, description="Filter by status: NEW or RE-ENTRY"),
    format: Optional[str] = Query(None),  # 'csv' for export
):
    """
    Returns apps that appeared during the current week window (last N snapshot dates),
    classified as NEW (never seen before week_start) or RE-ENTRY (seen before week_start
    but not present in previous week). Apps that were already present in the previous week
    are not included here.
    """
    con = connect()
    cur = con.cursor()

    # Base WHERE (chart_type + dims without dates)
    where_base, params_base = _where({"country": country, "category": category, "subcategory": subcategory})

    # Get the last N distinct dates (this week window)
    cur.execute(f"""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE {where_base}
        ORDER BY snapshot_date DESC LIMIT ?
    """, (*params_base, lookback_days))
    week_dates_desc = [r[0] for r in cur.fetchall()]
    if not week_dates_desc:
        con.close()
        return {
            "message": "No snapshots for the selected filters.",
            "rows": [],
            "counts": {"NEW": 0, "RE-ENTRY": 0},
            "week_start": None,
            "week_end": None,
        }

    week_dates = sorted(week_dates_desc)  # chronological
    week_start, week_end = week_dates[0], week_dates[-1]

    # Previous week (the N dates before week_start)
    cur.execute(f"""
        SELECT DISTINCT snapshot_date FROM charts
        WHERE {where_base} AND snapshot_date < ?
        ORDER BY snapshot_date DESC LIMIT ?
    """, (*params_base, week_start, lookback_days))
    prev_dates_desc = [r[0] for r in cur.fetchall()]
    prev_dates = set(prev_dates_desc)

    # Collect current week entries (first-seen-in-week date + rank on that date)
    placeholders_week = ",".join(["?"] * len(week_dates))
    cur.execute(f"""
        SELECT snapshot_date, app_id, app_name, rank, country, category, subcategory
        FROM charts
        WHERE {where_base} AND snapshot_date IN ({placeholders_week})
    """, (*params_base, *week_dates))

    week_seen: Dict[str, Dict[str, Any]] = {}
    for row in cur.fetchall():
        app_id = row[1]
        if not app_id:
            continue
        if app_id not in week_seen:
            week_seen[app_id] = {
                "first_seen_date": row[0],
                "rank": row[3],
                "app_id": app_id,
                "app_name": row[2],
                "country": row[4],
                "category": row[5],
                "subcategory": row[6],
            }

    # Who was present in previous week?
    if prev_dates:
        placeholders_prev = ",".join(["?"] * len(prev_dates))
        cur.execute(f"""
            SELECT DISTINCT app_id
            FROM charts
            WHERE {where_base} AND snapshot_date IN ({placeholders_prev})
        """, (*params_base, *list(prev_dates)))
        prev_week_ids = {r[0] for r in cur.fetchall() if r[0]}
    else:
        prev_week_ids = set()

    # Classify NEW vs RE-ENTRY
    rows: List[Dict[str, Any]] = []
    counts = {"NEW": 0, "RE-ENTRY": 0}

    for app_id, info in week_seen.items():
        if app_id in prev_week_ids:
            continue

        cur.execute(f"""
            SELECT 1 FROM charts
            WHERE {where_base} AND snapshot_date < ? AND app_id = ?
            LIMIT 1
        """, (*params_base, week_start, app_id))
        existed_before = cur.fetchone() is not None

        st = "RE-ENTRY" if existed_before else "NEW"
        counts[st] += 1
        rows.append({**info, "status": st})

    con.close()

    if status:
        status_u = status.strip().upper()
        rows = [r for r in rows if r["status"] == status_u]

    if (format or "").lower() == "csv":
        output = io.StringIO()
        w = csv.writer(output)
        w.writerow(["country", "category", "subcategory", "status", "first_seen_date", "rank", "app", "app_id"])
        for r in sorted(rows, key=lambda x: (x["rank"] is None, x["rank"] or 999)):
            w.writerow([
                r.get("country"), r.get("category"), r.get("subcategory"), r.get("status"),
                r.get("first_seen_date"), r.get("rank"), r.get("app_name"), r.get("app_id"),
            ])
        return Response(content=output.getvalue(), media_type="text/csv")

    return {
        "week_start": week_start,
        "week_end": week_end,
        "latest_snapshot": week_end,
        "counts": counts,
        "total": len(rows),
        "rows": sorted(rows, key=lambda x: (x["rank"] is None, x["rank"] or 999)),
    }
