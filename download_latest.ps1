param(
    [string]$repo = "Tsvetmila/appstore-scraper1",
    [string]$artifact = "appstore_charts_db",
    [string]$output = "appstore-api\data"
)

# Вземи номера на последния run
$runNumber = gh run list --repo $repo --limit 1 --json number --jq ".[0].number"

Write-Host "Последният успешен run number: $runNumber"

# Свали артефакта
gh run download $runNumber --repo $repo --name $artifact --dir $output

Write-Host "Файлът $artifact е изтеглен в $output"
