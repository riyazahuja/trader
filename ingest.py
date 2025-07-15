"""
ingest.py – news + market-data producer
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict

import aiohttp
import pandas as pd
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

import config

# ----------------------------------------------------------------------
# logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ----------------------------------------------------------------------
news_queue: asyncio.Queue[Dict] = asyncio.Queue()
bar_queue: asyncio.Queue[Dict] = asyncio.Queue()

NEWSAPI_URL = "https://newsapi.org/v2/everything"
FINNHUB_URL = "https://finnhub.io/api/v1/news"

seen_hashes: set[str] = set()


# ----------------------------------------------------------------------
async def fetch_news() -> None:
    """Poll NewsAPI + Finnhub and push deduped headlines to news_queue."""
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15)
    ) as session:
        while True:
            since_iso = (datetime.utcnow() - timedelta(minutes=20)).isoformat(
                timespec="seconds"
            )

            # ---- NewsAPI (single OR-query) --------------------------
            try:
                params = {
                    "apiKey": config.NEWSAPI_KEY,
                    "language": "en",
                    "pageSize": 50,
                    "q": " OR ".join(config.WATCHLIST),
                    "from": since_iso,
                    "sortBy": "publishedAt",
                }
                async with session.get(NEWSAPI_URL, params=params) as r:
                    data = await r.json()
                    for art in data.get("articles", []):
                        headline = art.get("title", "")
                        summary = art.get("description") or ""
                        if not headline:
                            continue
                        h = hashlib.md5(headline.encode()).hexdigest()
                        if h in seen_hashes:
                            continue
                        seen_hashes.add(h)
                        await news_queue.put(
                            {"key": h, "headline": headline, "summary": summary}
                        )
                        logger.info("Queued NewsAPI: %s", headline[:80])
            except Exception as exc:
                logger.warning("NewsAPI error: %s", exc)

            # ---- Finnhub (one request per ticker) -------------------
            for sym in config.WATCHLIST:
                try:
                    params = {
                        "token": config.FINNHUB_KEY,
                        "symbol": sym,
                        "from": since_iso.split("T")[0],  # YYYY-MM-DD
                    }
                    async with session.get(FINNHUB_URL, params=params) as r:
                        items = await r.json()
                        for art in items:
                            headline = art.get("headline", "")
                            summary = art.get("summary") or ""
                            if not headline:
                                continue
                            h = hashlib.md5(headline.encode()).hexdigest()
                            if h in seen_hashes:
                                continue
                            seen_hashes.add(h)
                            await news_queue.put(
                                {"key": h, "headline": headline, "summary": summary}
                            )
                            logger.info("Queued Finnhub: %s", headline[:80])
                except Exception as exc:
                    logger.warning("Finnhub error for %s: %s", sym, exc)

            await asyncio.sleep(300)  # 5 min pause


# ----------------------------------------------------------------------
async def fetch_bars(symbol: str) -> None:
    """Fetch the most recent 5-min bar batch for `symbol` every 60 s."""
    logger.info("Started bar loop for %s", symbol)
    client = StockHistoricalDataClient(
        api_key=config.APCA_API_KEY_ID,
        secret_key=config.APCA_API_SECRET_KEY,
    )

    while True:
        end = datetime.utcnow() - timedelta(minutes=16)  # 15-min free-tier rule
        start = end - timedelta(minutes=75)

        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame(5, TimeFrameUnit.Minute),
            start=start,
            end=end,
            feed=DataFeed.IEX,
        )
        try:
            bars = client.get_stock_bars(req).df

            if bars.empty:
                logger.warning(
                    "[%s] no bars returned (request window %s → %s)", symbol, start, end
                )
                await asyncio.sleep(60)
                continue

            # normalise index to tz‑naive DatetimeIndex
            if isinstance(bars.index, pd.MultiIndex):
                bars.index = bars.index.get_level_values(1)

            if isinstance(bars.index, pd.RangeIndex):
                # Alpaca can return RangeIndex when only one row; convert explicitly
                if "timestamp" in bars.columns:
                    bars.index = pd.to_datetime(bars["timestamp"])
                else:
                    bars.index = pd.to_datetime(bars.index)

            if isinstance(bars.index, pd.DatetimeIndex) and bars.index.tz is not None:
                bars.index = bars.index.tz_localize(None)

            await bar_queue.put({"symbol": symbol, "bars": bars})
            logger.debug(
                "[%s] %d bars (%s → %s)  bar_queue=%d",
                symbol,
                len(bars),
                bars.index.min(),
                bars.index.max(),
                bar_queue.qsize(),
            )
        except Exception as exc:
            logger.warning("Bar fetch error for %s: %s", symbol, exc)

        await asyncio.sleep(60)


# ----------------------------------------------------------------------
if __name__ != "__main__":
    logger.warning(
        "NOTE: ingest.py defines its own `news_queue` and `bar_queue` objects. "
        "When you run nlp_engine.py or selector.py in separate processes, "
        "they create *separate* queues, so nothing flows between them. "
        "Use an external broker (e.g. Redis, ZeroMQ, SQLite polling) or run all "
        "components in a single orchestrator process."
    )


async def main() -> None:
    # spin up one persistent fetch_bars task per symbol
    tasks = [asyncio.create_task(fetch_news())]
    tasks += [asyncio.create_task(fetch_bars(sym)) for sym in config.WATCHLIST]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
