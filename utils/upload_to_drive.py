# appstore-api/upload_to_drive.py
import os, glob, pathlib
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- 1) credentials от GitHub Secret (цялото JSON съдържание) ---
creds_json = os.getenv("GOOGLE_CREDS_JSON")
if not creds_json:
    raise RuntimeError("GOOGLE_CREDS_JSON secret/variable липсва")

creds_path = "/tmp/google_creds.json"
with open(creds_path, "w", encoding="utf-8") as f:
    f.write(creds_json)

creds = Credentials.from_service_account_file(creds_path)
drive = build("drive", "v3", credentials=creds)

# --- 2) ID на папката в Google Drive (сложи го като secret) ---
folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
if not folder_id:
    raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID secret/variable липсва")

# --- 3) Намиране на файла с базата независимо от разширението/пътя ---
candidates = [
    "appstore-api/data/app_data.db",
    "appstore-api/data/app_data.sqlite",
    "appstore-api/data/app_data.sqlite3",
    "appstore-api/data/app_data",
    "data/app_data.db",
    "data/app_data.sqlite",
    "data/app_data.sqlite3",
    "data/app_data",
]
db_file = next((p for p in candidates if os.path.exists(p)), None)
if db_file is None:
    # fallback: вземи каквото започва с app_data* в data/
    matches = sorted(glob.glob("appstore-api/data/app_data*")) + sorted(glob.glob("data/app_data*"))
    if matches:
        db_file = matches[0]

if db_file is None:
    raise FileNotFoundError(
        "Не намирам файла с базата. Пробвах: " + ", ".join(candidates)
    )

# име на файла в Drive (запазваме разширението, ако има)
ext = pathlib.Path(db_file).suffix or ".sqlite"
drive_name = "app_data" + ext

media = MediaFileUpload(db_file, mimetype="application/octet-stream", resumable=True)

# --- 4) Ако има вече файл със същото име в папката -> update, иначе create ---
query = f"'{folder_id}' in parents and name = '{drive_name}' and trashed = false"
resp = drive.files().list(q=query, fields="files(id,name)").execute()
files = resp.get("files", [])

if files:
    file_id = files[0]["id"]
    drive.files().update(fileId=file_id, media_body=media).execute()
    print(f"✅ Updated съществуващ файл '{drive_name}' ({file_id}) в Drive.")
else:
    meta = {"name": drive_name, "parents": [folder_id]}
    created = drive.files().create(body=meta, media_body=media, fields="id").execute()
    print(f"✅ Качих нов '{drive_name}' в Drive (file ID: {created['id']}).")

print(f"ℹ️ Локален файл: {db_file}")
