# appstore-api/gdrive_sync.py
import io
import json
import os
import sys
from typing import Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from tenacity import retry, stop_after_attempt, wait_fixed

FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
DB_LOCAL_PATH = os.getenv("DB_LOCAL_PATH", "appstore-api/data/app_data")
CREDS_JSON = os.getenv("GDRIVE_CREDENTIALS_JSON")

SCOPES = ["https://www.googleapis.com/auth/drive"]
MIME_BINARY = "application/octet-stream"
REMOTE_NAME = os.path.basename(DB_LOCAL_PATH)  # "app_data"

def _client():
    if not (FOLDER_ID and CREDS_JSON):
        raise RuntimeError("Missing env vars: GDRIVE_FOLDER_ID or GDRIVE_CREDENTIALS_JSON")

    info = json.loads(CREDS_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _ensure_local_dir():
    os.makedirs(os.path.dirname(DB_LOCAL_PATH), exist_ok=True)

def _find_remote_file_id(drive, name: str) -> Optional[str]:
    q = "name = @name and '{}' in parents and trashed = false".format(FOLDER_ID)
    res = drive.files().list(q=q, spaces="drive", fields="files(id, name)",
                             corpora="allDrives", includeItemsFromAllDrives=True,
                             supportsAllDrives=True,
                             pageSize=10, includePermissionsForView="published",
                             ).execute()
    files = res.get("files", [])
    for f in files:
        if f["name"] == name:
            return f["id"]
    return None

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
def pull():
    print("[gdrive] Pull start")
    _ensure_local_dir()
    drive = _client()
    file_id = _find_remote_file_id(drive, REMOTE_NAME)
    if not file_id:
        print(f"[gdrive] Remote file '{REMOTE_NAME}' not found in folder {FOLDER_ID}.")
        return False

    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(DB_LOCAL_PATH, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"[gdrive] Download {int(status.progress()*100)}%")
    print(f"[gdrive] Pulled to {DB_LOCAL_PATH}")
    return True

@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
def push():
    print("[gdrive] Push start")
    if not os.path.exists(DB_LOCAL_PATH):
        print(f"[gdrive] Local DB not found at {DB_LOCAL_PATH}; nothing to upload.")
        return False

    drive = _client()
    file_id = _find_remote_file_id(drive, REMOTE_NAME)
    media = MediaFileUpload(DB_LOCAL_PATH, mimetype=MIME_BINARY, resumable=True)

    if file_id:
        file = drive.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print(f"[gdrive] Updated remote file id={file['id']}")
    else:
        file_metadata = {"name": REMOTE_NAME, "parents": [FOLDER_ID]}
        file = drive.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True
        ).execute()
        print(f"[gdrive] Created remote file id={file['id']}")
    return True

if __name__ == "__main__":
    mode = (sys.argv[1] if len(sys.argv) > 1 else "pull").lower()
    if mode == "pull":
        ok = pull()
        sys.exit(0 if ok else 1)
    elif mode == "push":
        ok = push()
        sys.exit(0 if ok else 1)
    else:
        print("Usage: python gdrive_sync.py [pull|push]")
        sys.exit(2)
