# appstore-api/main.py
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, List, Dict, Any
from pathlib import Path
import os, sqlite3, csv, subprocess, sys
from io import StringIO

# --- Добавено: автоматично сваляне на базата от Google Drive при стартиране ---
import io, json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


def ensure_database_from_drive():
    local_path = "appstore-api/data/app_data.db"
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if os.path.exists(local_path):
        print("✅ Database already exists locally.")
        return

    if not creds_json or not folder_id:
        print("⚠️ Missing Google Drive credentials. Skipping DB download.")
        return

    print("⬇️ Database not found — downloading latest from Google Drive...")

    try:
        creds = service_account.Credentials.from_service_account_info(json.loads(creds_json))
        drive = build("drive", "v3", credentials=creds)
        query = f"'{folder_id}' in parents and name = 'app_data.db' and trashed = false"
        results = drive.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
        files = results.get("files", [])

        if not files:
            print("⚠️ No app_data.db found in Drive folder.")
            return

        file_id = files[0]["id"]
        request = drive.files().get_media(fileId=file_id)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with io.FileIO(local_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"⬇️ Download progress: {int(status.progress() * 100)}%")

        print(f"✅ Database downloaded to {local_path}")

        # --- 🆕 Добавена проверка и лог ---
        abs_path = os.path.abspath(local_path)
        if os.path.exists(local_path):
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"📂 Database found at {abs_path} ({size_mb:.2f} MB)")
        else:
            print(f"❌ Database file missing after download! Tried path: {abs_path}")

    except Exception as e:
        print(f"❌ Error downloading database: {e}")


# Извиква се веднага при стартиране на приложението (например в Render)
ensure_database_from_drive()

# --- Локализация на пътищата/базата -----------------------------------------
APP_DIR = Path(__file__).resolve().parent

_candidates: List[Optional[Path]] = [
    Path(os.getenv("DB_PATH")) if os.getenv("DB_PATH") else None,
    APP_DIR / "data" / "app_data.db",
    APP_DIR / "data" / "app_data.sqlite",
    APP_DIR / "data" / "app_data.sqlite3",
]


def _resolve_db_path() -> Path:
    for cand in _candidates:
        if cand and cand.exists():
            return cand
    return _candidates[1]


DB_PATH = _resolve_db_path()

# -----------------------------------------------------------------------------
app = FastAPI(title="AppStore Charts API", version="1.1")

# --- CORS --------------------------------------------------------------------
_default_origins = {
    "https://appstore-scraper1.vercel.app",
    "https://appstore-scraper1-git-main-tsvetemilias-projects.vercel.app",  # 👈 добавен Vercel домейн
    "http://localhost:5173",
    "http://127.0.0.1:5173",
}
_env_origins = {o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()}
ALLOWED_ORIGINS = sorted((_default_origins | _env_origins) - {""})

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# --- Помощни функции ----------------------------------------------------------
def connect() -> sqlite3.Connection:
    if not Path(DB_PATH).exists():
        tried = [str(c) for c in _candidates if c is not None]
        raise HTTPException(
            status_code=503,
            detail={"error": "Database file not found", "tried_paths": tried},
        )
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con

# (останалата логика остава без промени — compare7, meta, reports, run-scraper и т.н.)
