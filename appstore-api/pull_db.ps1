param(
  [string]$Repo = "Tsvetmila/appstore-scraper1"   # смени ако е друго repo
)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$targetDir = Join-Path $here "data"
if (!(Test-Path $targetDir)) { New-Item -ItemType Directory -Path $targetDir | Out-Null }

Write-Host "Downloading latest database artifact from $Repo..."

# Взимаме ID на последния успешен рън на workflow "Daily App Store Scrape"
$run = gh run list --repo $Repo --workflow "Daily App Store Scrape" --json databaseId,status,conclusion,createdAt -L 1 | ConvertFrom-Json
if (-not $run) { Write-Error "No runs found"; exit 1 }

$runId = $run[0].databaseId
Write-Host "Latest run id: $runId, status=$($run[0].conclusion)"

# Сваляне на артефакта
$tmp = Join-Path $here "_tmp_artifact"
if (Test-Path $tmp) { Remove-Item -Recurse -Force $tmp }
gh run download $runId --repo $Repo --name "app_data.db" --dir $tmp

# Копираме като appcharts.db в data\
$src = Join-Path $tmp "app_data.db"
$dst = Join-Path $targetDir "appcharts.db"
Copy-Item $src $dst -Force

# Почисти
Remove-Item -Recurse -Force $tmp

Write-Host "Database updated: $dst"
