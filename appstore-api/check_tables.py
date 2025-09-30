import sqlite3

p = "app_data.db"
con = sqlite3.connect(p)

tables = [t[0] for t in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("Таблици в базата:", tables)
