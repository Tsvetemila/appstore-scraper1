from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import uvicorn

app = FastAPI(title="App Store Top 50 API")

# CORS настройка
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite фронтенд
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Примерни данни (тук по-късно ще влезе твоят Colab код)
SAMPLE_APPS = [
    {
        "name": "ChatGPT",
        "developer": "OpenAI",
        "category": "Overall",
        "app_store_url": "https://apps.apple.com/app/chatgpt/id6448311069",
        "icon_url": "https://is1-ssl.mzstatic.com/image/thumb/Purple211/v4/65/48/65/654865b1-ff91-2df7-dbc5-aaaabbccdde0/AppIcon-0-1x_U007emarketing-0-10-0-85-220.png/100x100bb.png",
        "current_rank": 1,
    },
    {
        "name": "Instagram",
        "developer": "Meta",
        "category": "Social",
        "app_store_url": "https://apps.apple.com/app/instagram/id389801252",
        "icon_url": "https://is1-ssl.mzstatic.com/image/thumb/Purple221/v4/3b/29/bd/3b29bd72-1111-2222-3333-abcdef123456/AppIcon-0-1x.png/100x100bb.png",
        "current_rank": 2,
    },
]

@app.get("/top50")
async def get_top50(
    country: str = Query(..., description="Country code (US, GB, DE, FR, JP)"),
    category: str = Query(..., description="App category (Overall, Games, Music, Social)")
):
    try:
        # 👉 Тук по-късно ще влезе реалния ти код от Colab
        apps = SAMPLE_APPS  

        # Връщаме JSON в точния формат
        return {"apps": apps, "country": country, "category": category}

    except Exception as e:
        return {"apps": [], "error": str(e)}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
