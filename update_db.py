import requests, zipfile, io, os

# Настройки
OWNER = "Tsvetmila"
REPO = "appstore-scraper1"
ARTIFACT_NAME = "app_data_db"
DEST_PATH = "appstore-api/data/app_data.db"
TOKEN = os.getenv("GITHUB_TOKEN")  # ще го добавим после

headers = {"Authorization": f"token {TOKEN}"} if TOKEN else {}

print("📥 Fetching latest artifact...")
url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/artifacts"
resp = requests.get(url, headers=headers)
resp.raise_for_status()
artifacts = resp.json().get("artifacts", [])

latest = next((a for a in artifacts if a["name"] == ARTIFACT_NAME), None)
if not latest:
    raise SystemExit("❌ Artifact not found.")

download_url = latest["archive_download_url"]
print(f"⬇️ Downloading {ARTIFACT_NAME}...")

z = requests.get(download_url, headers=headers)
z.raise_for_status()

with zipfile.ZipFile(io.BytesIO(z.content)) as zf:
    name = zf.namelist()[0]
    with zf.open(name) as fsrc, open(DEST_PATH, "wb") as fdst:
        fdst.write(fsrc.read())

print(f"✅ Database updated at {DEST_PATH}")
