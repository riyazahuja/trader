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
from alpaca.data.enums import DataFeed

import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

news_queue: asyncio.Queue = asyncio.Queue()
bar_queue: asyncio.Queue = asyncio.Queue()


# --- imports (top of file) ---------------------------------------
import hashlib, time, requests
from datetime import datetime, timedelta

NEWS_ENDPOINTS = [
    (
        "https://newsapi.org/v2/everything",
        {"apiKey": config.NEWSAPI_KEY, "language": "en", "pageSize": 50},
    ),
    (
        "https://finnhub.io/api/v1/news",
        {"token": config.FINNHUB_KEY, "category": "general"},
    ),
]

seen_hashes: set[str] = set()


async def fetch_news():
    """Poll endpoints every 5 min and push deduplicated items to news_queue."""
    while True:
        since = (datetime.utcnow() - timedelta(minutes=15)).isoformat(
            timespec="seconds"
        )
        for url, base_params in NEWS_ENDPOINTS:
            params = base_params.copy()
            # params.update(q=" OR ".join(config.WATCHLIST), from_param=since)
            params["q"] = " OR ".join(config.WATCHLIST)
            params["from"] = since
            try:
                resp = requests.get(url, params=params, timeout=10)
                resp.raise_for_status()
                items = resp.json().get(
                    "articles", resp.json()
                )  # Finnhub has no 'articles'
                for art in items:
                    h = hashlib.md5(art["title"].encode()).hexdigest()
                    if h in seen_hashes:
                        continue  # dedupe headline
                    seen_hashes.add(h)
                    await news_queue.put(
                        {
                            "key": h,
                            "headline": art["title"],
                            "summary": art.get("description") or "",
                        }
                    )
                    logger.info("Queued news: %s", art["title"][:60])
            except Exception as exc:
                logger.warning("News fetch error: %s", exc)
        await asyncio.sleep(300)  # 5 min


def dedup_key(item: Dict) -> str:
    return hashlib.md5(
        (item.get("headline", "") + item.get("summary", "")).encode()
    ).hexdigest()


from alpaca.data.timeframe import TimeFrame, TimeFrameUnit


async def fetch_bars(symbol: str):
    logger.info("Started bar fetch loop for %s", symbol)

    client = StockHistoricalDataClient(
        api_key=config.APCA_API_KEY_ID, secret_key=config.APCA_API_SECRET_KEY
    )
    end = datetime.utcnow() - timedelta(minutes=16)
    start = end - timedelta(minutes=75)
    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame(5, TimeFrameUnit.Minute),  # 5-min bars per spec
        start=start,
        end=end,
        feed=DataFeed.IEX,
    )
    try:
        bars = client.get_stock_bars(request).df
        logger.info("[%s] fetched %d bars (%s → %s)", symbol, len(bars), start, end)
    except Exception as exc:
        logger.error("Bar fetch error for %s: %s", symbol, exc)
        return
    # bars.index = bars.index.tz_localize(None)
    if isinstance(bars.index, pd.MultiIndex):
        bars.index = bars.index.get_level_values(1)  # keep only the timestamp
    bars.index = bars.index.tz_localize(None)
    await bar_queue.put({"symbol": symbol, "bars": bars})
    logger.debug("[%s] bar_queue size=%d", symbol, bar_queue.qsize())


# async def fetch_news(symbol: str):
#     url = "https://newsapi.org/v2/everything"
#     params = {
#         "q": symbol,
#         "apiKey": config.NEWSAPI_KEY,
#         "pageSize": 5,
#         "sortBy": "publishedAt",
#     }
#     resp = requests.get(url, params=params, timeout=10)
#     resp.raise_for_status()
#     for article in resp.json().get("articles", []):
#         item = {
#             "headline": article.get("title"),
#             "summary": article.get("description"),
#             "timestamp": article.get("publishedAt"),
#         }
#         item["key"] = dedup_key(item)
#         await news_queue.put(item)


async def main():
    while True:
        tasks = []
        tasks.append(asyncio.create_task(fetch_news()))

        for sym in config.WATCHLIST:
            tasks.append(fetch_bars(sym))
            # tasks.append(fetch_news(sym))
        await asyncio.gather(*tasks)
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
