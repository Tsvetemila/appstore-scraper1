import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Вземи JSON от секретната променлива (GOOGLE_CREDS_JSON)
creds_json = os.getenv("GOOGLE_CREDS_JSON")

if not creds_json:
    raise Exception("GOOGLE_CREDS_JSON environment variable not found")

# Запиши го като временен файл
creds_path = "/tmp/google_creds.json"
with open(creds_path, "w") as f:
    f.write(creds_json)

# Създай credentials обект
credentials = service_account.Credentials.from_service_account_file(creds_path)
service = build('drive', 'v3', credentials=credentials)

# Определи файла за качване
file_path = "appstore-api/data/app_data.db"
file_metadata = {
    "name": "app_data.sqlite",
    "parents": ["15R7mtQqUfKs5Cz4JgBbw8ySYKx-AnRlF"]  # ID на твоята Google Drive папка
}

media = MediaFileUpload(file_path, resumable=True)
file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()

print(f"✅ Uploaded {file_path} to Google Drive (file ID: {file.get('id')})")
