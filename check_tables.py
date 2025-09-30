import sqlite3

# смени пътя, ако трябва
db_path = r".\appstore-api\data\appcharts.db"

con = sqlite3.connect(db_path)
tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tables:", tables)

for (t,) in tables:
    print(f"\n--- {t} ---")
    cols = con.execute(f"PRAGMA table_info({t})").fetchall()
    for col in cols:
        print(col)
