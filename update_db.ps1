# update_db.ps1
cd "C:\Users\Tsveti-PC\appstore-automation"

# Дърпаме последните промени от GitHub
git pull origin main

# Записваме лог за проверка
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
"Database updated at $timestamp" | Out-File -Append "C:\Users\Tsveti-PC\appstore-automation\update_db.log"
