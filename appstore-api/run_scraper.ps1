# run_scraper.ps1 — пуска scraper.py от текущата папка и логва изхода

# 1) Работна директория = папката на скрипта (appstore-api)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

# 2) Логове
$logDir = Join-Path $ScriptDir 'logs'
if (!(Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$ts  = Get-Date -Format 'yyyy-MM-dd_HH-mm-ss'
$log = Join-Path $logDir "scraper_$ts.log"

# 3) Избор на Python: ако има .venv/ го ползва, иначе py launcher
$venvPy = Join-Path $ScriptDir '.venv\Scripts\python.exe'
$python = (Test-Path $venvPy) ? $venvPy : 'py'

"=== RUN $(Get-Date -Format o) ===" | Out-File -FilePath $log -Encoding utf8
"WorkingDir: $ScriptDir"              | Out-File -Append -FilePath $log
"Python: $python"                     | Out-File -Append -FilePath $log

# 4) Стартиране на scraper.py и логване на stdout/stderr
try {
    & $python 'scraper.py' *>> $log
    $exitCode = $LASTEXITCODE
} catch {
    $_ | Out-File -Append -FilePath $log
    $exitCode = 1
}

"ExitCode=$exitCode" | Out-File -Append -FilePath $log
exit $exitCode
