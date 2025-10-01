# init_db.py
import os, sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "app_data.db")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS charts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Контекст на снимката (snapshot)
    snapshot_date   TEXT    NOT NULL,               -- 'YYYY-MM-DD' (UTC или BG, но последователно)
    country         TEXT    NOT NULL,               -- 'BG', 'US' и т.н.
    chart_type      TEXT    NOT NULL,               -- 'top_free' | 'top_paid' | 'top_grossing'
    category        TEXT    NOT NULL,               -- основна категория (напр. 'Games')
    subcategory     TEXT,                            -- подкатегория (напр. 'Arcade')

    -- Данни за класирането
    rank            INTEGER NOT NULL,               -- 1..N (обикновено 1..50)
    app_id          TEXT    NOT NULL,               -- Apple numeric id като текст (по-надеждно за SQLite)
    bundle_id       TEXT,                           -- com.company.app
    app_name        TEXT    NOT NULL,
    developer_name  TEXT,

    -- Допълнителни полета (по желание)
    price           REAL,
    currency        TEXT,
    rating          REAL,
    ratings_count   INTEGER,

    fetched_at      TEXT DEFAULT (datetime('now')), -- кога е извлечено
    raw             TEXT                             -- суров JSON ако искаш да държиш целия отговор
);

-- Уникален ранг за контекст в даден ден (не може две приложения на една и съща позиция)
CREATE UNIQUE INDEX IF NOT EXISTS ux_charts_context_rank
ON charts (snapshot_date, country, chart_type, category, subcategory, rank);

-- Предпазна мрежа: едно и също приложение да не се дублира в един и същи контекст за деня
CREATE UNIQUE INDEX IF NOT EXISTS ux_charts_context_app
ON charts (snapshot_date, country, chart_type, category, subcategory, app_id);

-- Индекси за бързи заявки
CREATE INDEX IF NOT EXISTS idx_charts_date
ON charts (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_charts_app_date
ON charts (app_id, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_charts_ctx_date
ON charts (country, chart_type, category, subcategory, snapshot_date);

-- Вю: текущ топ 50 (последната налична дата)
CREATE VIEW IF NOT EXISTS v_current_top50 AS
SELECT *
FROM charts
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM charts)
  AND rank <= 50;

-- Вю: всички редове от последните 7 дни спрямо последната налична дата
CREATE VIEW IF NOT EXISTS v_last7 AS
SELECT *
FROM charts
WHERE snapshot_date >= date((SELECT MAX(snapshot_date) FROM charts), '-6 day');
"""

con = sqlite3.connect(DB_PATH)
con.executescript(SCHEMA)
con.commit()
con.close()

print("OK: schema ready at", DB_PATH)
