import asyncio
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
import requests
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

news_queue: asyncio.Queue = asyncio.Queue()
bar_queue: asyncio.Queue = asyncio.Queue()


def dedup_key(item: Dict) -> str:
    return hashlib.md5((item.get("headline", "") + item.get("summary", "")).encode()).hexdigest()


async def fetch_bars(symbol: str):
    client = StockHistoricalDataClient(
        api_key=config.APCA_API_KEY_ID, secret_key=config.APCA_API_SECRET_KEY
    )
    end = datetime.utcnow()
    start = end - timedelta(minutes=60)
    request = StockBarsRequest(
        symbol_or_symbols=symbol, timeframe=TimeFrame.Minute, start=start, end=end
    )
    bars = client.get_stock_bars(request).df
    bars.index = bars.index.tz_localize(None)
    await bar_queue.put({"symbol": symbol, "bars": bars})


async def fetch_news(symbol: str):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": symbol,
        "apiKey": config.NEWSAPI_KEY,
        "pageSize": 5,
        "sortBy": "publishedAt",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    for article in resp.json().get("articles", []):
        item = {
            "headline": article.get("title"),
            "summary": article.get("description"),
            "timestamp": article.get("publishedAt"),
        }
        item["key"] = dedup_key(item)
        await news_queue.put(item)


async def main():
    while True:
        tasks = []
        for sym in config.WATCHLIST:
            tasks.append(fetch_bars(sym))
            tasks.append(fetch_news(sym))
        await asyncio.gather(*tasks)
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
