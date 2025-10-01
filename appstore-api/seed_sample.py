import sqlite3

rows = [
    ("2025-09-30", "BG", "top_free", "Games", "Arcade", 1, "1234567890", "com.demo.arcade", "Arcade Blast", "DemoDev", 0.0, "BGN", 4.7, 1200),
    ("2025-09-30", "BG", "top_free", "Games", "Arcade", 2, "2222222222", "com.demo.runner", "Runner Pro", "DemoDev", 0.0, "BGN", 4.5, 800),
    ("2025-09-29", "BG", "top_free", "Games", "Arcade", 1, "2222222222", "com.demo.runner", "Runner Pro", "DemoDev", 0.0, "BGN", 4.5, 780),
    ("2025-09-29", "BG", "top_free", "Games", "Arcade", 2, "1234567890", "com.demo.arcade", "Arcade Blast", "DemoDev", 0.0, "BGN", 4.7, 1190),
]

con = sqlite3.connect("app_data.db")
con.executemany("""
INSERT OR REPLACE INTO charts
(snapshot_date, country, chart_type, category, subcategory, rank, app_id, bundle_id, app_name, developer_name, price, currency, rating, ratings_count)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", rows)
con.commit()
con.close()

print("Seed OK")
