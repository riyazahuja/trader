import asyncio
import json
import logging
from typing import Dict

import google.generativeai as genai

import config
from ingest import news_queue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

genai.configure(api_key=config.GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

analysis_queue: asyncio.Queue = asyncio.Queue()

PROMPT = (
    "Extract from this headline+summary:\n"
    " • Ticker(s)\n"
    " • Asset class [penny_stock|large_cap_stock]\n"
    " • Sentiment [positive|negative|neutral]\n"
    " • Urgency on scale 0–1\n"
    " • Expected move (%)\n"
    "Output JSON."
)


async def analyze(item: Dict):
    logger.info("Analyzing news key=%s headline=%s", item["key"], item["headline"][:60])
    tries = 0
    while tries < 3:
        try:
            response = model.generate_content(
                f"{item['headline']}\n{item['summary']}\n{PROMPT}"
            )
            text = response.text
            data = json.loads(text)
            data["key"] = item["key"]
            logger.info(
                "Analysis %s: %s %s%% move",
                data["tickers"],
                data["sentiment"],
                data.get("expected_move", "?"),
            )
            await analysis_queue.put(data)
            logger.info(
                "Queued analysis for %s (analysis_queue=%d)",
                data["key"],
                analysis_queue.qsize(),
            )
            return
        except Exception as exc:  # handle rate limits
            tries += 1
            await asyncio.sleep(1 + tries)
            logger.warning("LLM error %s", exc)


async def main():
    while True:
        print("Waiting for news item...")
        item = await news_queue.get()
        logger.debug("news_queue size now %d", news_queue.qsize())
        await analyze(item)


if __name__ == "__main__":
    asyncio.run(main())
