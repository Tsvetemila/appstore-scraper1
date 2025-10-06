import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 🔹 Взимаме JSON съдържанието от GitHub Secret (GOOGLE_CREDENTIALS)
google_creds_json = os.environ.get("GOOGLE_CREDENTIALS")

if not google_creds_json:
    raise ValueError("❌ Missing GOOGLE_CREDENTIALS environment variable.")

# 🔹 Парсваме JSON съдържанието
creds_dict = json.loads(google_creds_json)

# 🔹 Настройваме Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.file']
credentials = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

# 🔹 Път до базата
file_path = '/data/app_data'
file_metadata = {'name': 'app_data.sqlite'}
media = MediaFileUpload(file_path, mimetype='application/octet-stream')

# 🔹 Проверяваме дали вече има файл с това име
files = service.files().list(q="name='app_data.sqlite'", fields="files(id)").execute().get('files', [])
if files:
    file_id = files[0]['id']
    service.files().update(fileId=file_id, media_body=media).execute()
    print("✅ Updated existing app_data.sqlite on Google Drive")
else:
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print("✅ Uploaded new app_data.sqlite to Google Drive")
