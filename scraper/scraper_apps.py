import os, time, json, sqlite3, requests
from datetime import datetime
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "appstore-api", "data", "app_data.db")

COUNTRIES = ["US", "GB", "FR", "DE", "ES", "RU", "IT", "CA"]

APP_CATEGORIES = {
    "books": 6018, "business": 6000, "developer-tools": 6026, "education": 6017,
    "entertainment": 6016, "finance": 6015, "food-drink": 6023, "graphics-design": 6027,
    "health-fitness": 6013, "kids": 6061, "lifestyle": 6012, "magazines-newspapers": 6021,
    "medical": 6020, "music": 6011, "navigation": 6010, "news": 6009, "photo-video": 6008,
    "productivity": 6007, "reference": 6006, "shopping": 6024, "social-networking": 6005,
    "sports": 6004, "travel": 6003, "utilities": 6002, "weather": 6001,
}

HTTP_TIMEOUT, HTTP_RETRIES = 10, 3

def http_get(url: str):
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "charts-bot/1.0"})
            if r.status_code == 200: return r
            if r.status_code == 404: return None
        except Exception as e:
            print(f"[WARN] {attempt}/{HTTP_RETRIES} {url}: {e}")
        time.sleep(1.2 * attempt)
    return None

def http_get_json(url: str):
    r = http_get(url)
    return r.json() if r else None

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS charts (
            snapshot_date TEXT, country TEXT, category TEXT, subcategory TEXT,
            chart_type TEXT, rank INTEGER, app_id TEXT, bundle_id TEXT,
            app_name TEXT, developer_name TEXT, price REAL, currency TEXT,
            rating REAL, ratings_count INTEGER, genre_id TEXT, raw TEXT,
            PRIMARY KEY (snapshot_date,country,category,subcategory,chart_type,rank)
        )
    """)
    cur.execute("PRAGMA table_info(charts)")
    cols = [r[1] for r in cur.fetchall()]
    if "genre_id" not in cols:
        cur.execute("ALTER TABLE charts ADD COLUMN genre_id TEXT;")
    cur.execute("PRAGMA table_info(charts)")
    cols = [r[1] for r in cur.fetchall()]
    for col in ["app_store_url", "app_url", "icon_url"]:
        if col not in cols:
            print(f"[DB] Adding missing column: {col}")
            cur.execute(f"ALTER TABLE charts ADD COLUMN {col} TEXT;")
    conn.commit()

def insert_rows(conn, rows):
    conn.executemany("""
        INSERT OR REPLACE INTO charts (
            snapshot_date,country,category,subcategory,chart_type,rank,
            app_id,bundle_id,app_name,developer_name,price,currency,
            rating,ratings_count,genre_id,
            app_store_url,app_url,icon_url,raw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
    conn.commit()

def parse_itunes(data):
    out=[]
    for i,e in enumerate(data.get("feed",{}).get("entry",[]) or [],1):
        id_url=(e.get("id") or {}).get("label") or ""
        app_id=id_url.split("/id")[-1].split("?")[0] if "/id" in id_url else ""
        name=((e.get("im:name") or {}).get("label")) or (e.get("title") or {}).get("label")
        artist=((e.get("im:artist") or {}).get("label")) or ""
        out.append({"rank":i,"id":app_id,"name":name,"artistName":artist})
    return out

def parse_html_apps(country, genre_id, slug):
    url=f"https://apps.apple.com/{country.lower()}/charts/iphone/{slug}-apps/{genre_id}?chart=top-free"
    r=http_get(url)
    if not r: return []
    soup=BeautifulSoup(r.text,"lxml")
    items=[]
    for i,li in enumerate(soup.select("li div.we-lockup__content"),1):
        a=li.find("a",href=True)
        name=a.get_text(strip=True)
        artist=li.find("div",class_="we-lockup__subtitle").get_text(strip=True)
        href=a["href"]
        app_id=href.split("/id")[-1].split("?")[0] if "/id" in href else ""
        if app_id: items.append({"rank":i,"id":app_id,"name":name,"artistName":artist})
    if items: print(f"[HTML] Parsed {len(items)} apps from {url}")
    return items

def fetch_genre_top50(country, genre_id, slug):
    data=http_get_json(f"https://itunes.apple.com/{country.lower()}/rss/topfreeapplications/limit=50/genre={genre_id}/json")
    items=parse_itunes(data) if data else []
    if items: return items,"itunes"
    # fallback HTML
    return parse_html_apps(country, genre_id, slug),"html"



def enrich_with_lookup(country, ids):
    out={}
    for i in range(0,len(ids),50):
        chunk=[x for x in ids[i:i+50] if x]
        if not chunk: continue
        url=f"https://itunes.apple.com/lookup?id={','.join(chunk)}&country={country}"
        data=http_get_json(url) or {}
        for r in data.get("results",[]):
            tid=str(r.get("trackId") or "")
            if not tid: continue
            out[tid]={
             "bundle_id": r.get("bundleId"),
             "price": r.get("price"),
             "currency": r.get("currency"),
             "rating": r.get("averageUserRating"),
             "ratings_count": r.get("userRatingCount"),
             "app_store_url": r.get("trackViewUrl"),
             "app_url": r.get("sellerUrl"),
             "icon_url": r.get("artworkUrl100"),
             "raw": json.dumps(r, ensure_ascii=False)
            }
    return out

def scrape_apps():
    snap=datetime.utcnow().date().isoformat()
    conn=sqlite3.connect(DB_PATH)
    ensure_schema(conn)
    total=0
    for country in COUNTRIES:
        for slug,gid in APP_CATEGORIES.items():
            items,src=fetch_genre_top50(country,gid,slug)
            if not items: 
                print(f"[INFO] Empty {country}/{gid} ({slug})")
                continue
            lookup=enrich_with_lookup(country,[i["id"] for i in items])
            rows=[(snap,country,slug.replace("-"," ").title(),None,"top_free",
                    it["rank"],it["id"],lookup.get(it["id"],{}).get("bundle_id"),
                    it["name"],it["artistName"],
                    lookup.get(it["id"], {}).get("price"),
                    lookup.get(it["id"], {}).get("currency"),
                    lookup.get(it["id"], {}).get("rating"),
                    lookup.get(it["id"], {}).get("ratings_count"),
                    lookup.get(it["id"], {}).get("genre_id"),
                    lookup.get(it["id"], {}).get("app_store_url"),
                    lookup.get(it["id"], {}).get("app_url"),
                    lookup.get(it["id"], {}).get("icon_url"),
                    lookup.get(it["id"], {}).get("raw"))
                   for it in items]
            insert_rows(conn,rows)
            total+=len(rows)
            print(f"[INFO] {country} {slug} ({src}): {len(rows)}")
    
    print(f"[OK] APPS inserted {total} rows {snap}")


    # ✅ Затваряме базата безопасно
    if conn:
        conn.close()


if __name__=="__main__":
    scrape_apps()
