\# App Store Top 50 (Automation)



\- Scraper за Apple App Store Top Free (Top 50) за избрани държави и категории.

\- Резултатът се записва в SQLite база `appstore\_charts.db` и CSV файлове.

\- GitHub Actions пуска скрейпа автоматично по график и качва артефакти (DB+CSV).



\## Как да стартирам локално



```bash

py -m venv .venv

.venv\\Scripts\\Activate.ps1

pip install -r scraper/requirements.txt

py scraper/scraper.py



