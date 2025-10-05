import sqlite3, shutil

old_db = "app_data_old.db"   # вчера
new_db = "app_data_new.db"   # днес
merged_db = "app_data.db"    # финален резултат

# 1️⃣ Копираме старата база като основна
shutil.copyfile(old_db, merged_db)

src = sqlite3.connect(new_db)
dst = sqlite3.connect(merged_db)

src_cur = src.cursor()
dst_cur = dst.cursor()

# 2️⃣ Взимаме колоните, но пропускаме ID (за да се генерира ново)
src_cols = [r[1] for r in src_cur.execute("PRAGMA table_info(charts)") if r[1] != "id"]
dst_cols = [r[1] for r in dst_cur.execute("PRAGMA table_info(charts)") if r[1] != "id"]
common_cols = [c for c in src_cols if c in dst_cols]

print(f"🧩 Matching columns: {len(common_cols)} (without id)")

cols_str = ",".join(common_cols)
placeholders = ",".join("?" * len(common_cols))

# 3️⃣ Извличаме само snapshot_date = '2025-10-05'
rows = src_cur.execute(
    f"SELECT {cols_str} FROM charts WHERE snapshot_date = '2025-10-05'"
).fetchall()

print(f"📦 Extracted {len(rows)} rows for 2025-10-05")

# 4️⃣ Вмъкваме безопасно – SQLite ще генерира нови id
insert_sql = f"INSERT INTO charts ({cols_str}) VALUES ({placeholders})"
dst_cur.executemany(insert_sql, rows)
dst.commit()

# 5️⃣ Проверяваме snapshot датите
snapshots = dst_cur.execute(
    "SELECT DISTINCT snapshot_date FROM charts ORDER BY snapshot_date"
).fetchall()
print(f"✅ Final snapshots in merged DB: {snapshots}")

src.close()
dst.close()
