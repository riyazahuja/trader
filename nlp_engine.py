import asyncio
import json
import logging
import re
from json import JSONDecodeError
from typing import Dict, List

from pydantic import BaseModel, Field, ValidationError
import google.generativeai as genai

import config
from ingest import news_queue  # we don't use bar_queue here

# ---------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

genai.configure(api_key=config.GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

analysis_queue: asyncio.Queue = asyncio.Queue()


# ---------------------------------------------------------------------
class NewsAnalysis(BaseModel):
    tickers: List[str] = Field(description="Ticker symbols mentioned in the news")
    asset_class: str = Field(description="penny_stock or large_cap_stock")
    sentiment: str = Field(description="positive, negative, or neutral")
    urgency: float = Field(ge=0.0, le=1.0, description="Urgency on 0-1 scale")
    expected_move: float | None = Field(
        default=None, description="Expected absolute percentage move"
    )


async def analyze(item: Dict):
    logger.info("Analyzing news key=%s headline=%s", item["key"], item["headline"][:60])

    prompt = (
        f"{item['headline']}\n{item['summary']}\n\n"
        "Return a JSON object matching this schema:\n"
        "{"
        ' "tickers": array<string>,'
        ' "asset_class": "penny_stock" | "large_cap_stock",'
        ' "sentiment": "positive" | "negative" | "neutral",'
        ' "urgency": number(0-1),'
        ' "expected_move": number'
        "}"
    )

    tries, raw_text = 0, ""
    while tries < 3:
        try:
            response = model.generate_content(
                prompt,
                generation_config={
                    "response_mime_type": "application/json",  # tell Gemini to emit pure JSON
                    # "response_schema": NewsAnalysis,  # Pydantic schema
                },
            )
            # print(">>> done generating")

            raw_text = response.text.strip()
            print(f"Raw response: {raw_text}")
            # If validation succeeded, we get a parsed NewsAnalysis instance
            obj: NewsAnalysis
            if response.text:
                obj = response.text  # type: ignore
            else:
                # Fallback in case SDK couldn't auto-parse (rare)
                cleaned = re.sub(r"```(json)?|```", "", raw_text).strip()
                obj = NewsAnalysis.model_validate_json(cleaned)

            data = json.loads(obj)
            data["key"] = item["key"]
            data["tickers"] = [t.upper() for t in data["tickers"]]

            logger.info(
                "Analysis %s: %s %.2f%% move",
                data["tickers"],
                data["sentiment"],
                data.get("expected_move", float("nan")),
            )
            await analysis_queue.put(data)
            logger.info(
                "Queued analysis for %s (analysis_queue=%d)",
                data["key"],
                analysis_queue.qsize(),
            )
            return
        except (ValidationError, JSONDecodeError) as exc:
            tries += 1
            logger.warning("JSON validation error (%s). Raw: %s", exc, raw_text[:120])
        except Exception as exc:
            tries += 1
            logger.warning("LLM/API error %s (try %d/3)", exc, tries)

        await asyncio.sleep(1 + tries)

    logger.error(
        "Failed to parse structured output for news key=%s after 3 tries", item["key"]
    )


async def main():
    while True:
        item = await news_queue.get()
        await analyze(item)


if __name__ == "__main__":
    asyncio.run(main())
