import os
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

def main():
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    local_path = "appstore-api/data/app_data.db"

    if not creds_json or not folder_id:
        print("❌ Missing Google credentials or folder ID.")
        return

    creds = service_account.Credentials.from_service_account_info(json.loads(creds_json))
    drive = build("drive", "v3", credentials=creds)

    # Търсим най-новия файл app_data.db в папката
    query = f"'{folder_id}' in parents and name = 'app_data.db' and trashed = false"
    results = drive.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    files = results.get("files", [])

    if not files:
        print("⚠️ No existing app_data.db found in Drive. Skipping download.")
        return

    file_id = files[0]["id"]
    file_name = files[0]["name"]
    print(f"⬇️ Found existing {file_name}. Downloading latest version...")

    request = drive.files().get_media(fileId=file_id)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    with io.FileIO(local_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"  Download progress: {int(status.progress() * 100)}%")

    print(f"✅ Download complete: {local_path}")

if __name__ == "__main__":
    main()
