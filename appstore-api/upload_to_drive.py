from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = '/etc/secrets/google_creds.json'

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

file_path = '/data/app_data'
file_metadata = {'name': 'app_data.sqlite'}
media = MediaFileUpload(file_path, mimetype='application/octet-stream')

files = service.files().list(q="name='app_data.sqlite'", fields="files(id)").execute().get('files', [])
if files:
    file_id = files[0]['id']
    service.files().update(fileId=file_id, media_body=media).execute()
    print("✅ Updated existing app_data.sqlite on Drive")
else:
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print("✅ Uploaded new app_data.sqlite to Drive")
