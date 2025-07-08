import asyncio
import logging
from math import floor
from typing import Dict

import pandas as pd
from alpaca.trading.client import TradingClient

import config
from ingest import bar_queue
from nlp_engine import analysis_queue
from indicators import add_indicators

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

candidate_queue: asyncio.Queue = asyncio.Queue()

trading_client = TradingClient(
    api_key=config.APCA_API_KEY_ID,
    secret_key=config.APCA_API_SECRET_KEY,
    paper=True,
)


def passes_filters(row: pd.Series, analysis: Dict) -> bool:
    if analysis.get("sentiment", "") == "negative":
        logger.debug("%s filtered out: negative sentiment", row.name)
        return False
    if analysis.get("urgency", 0) < 0.5:
        logger.debug("%s filtered out: urgency < 0.5", row.name)
        return False
    # if analysis.get("sentiment", 0) < 0.7 and analysis.get("urgency", 0) < 0.5:
    #     return False
    if row["volume"] < 3 * row["VOL_AVG5"]:
        logger.debug("%s filtered out: volume < 3x avg", row.name)
        return False
    if row["close"] <= row["EMA20"] or row["close"] <= row["VWAP"]:
        logger.debug("%s filtered out: close <= EMA20 or VWAP", row.name)
        return False
    return True


async def process(symbol: str, bars: pd.DataFrame, analysis: Dict):
    enriched = add_indicators(bars)
    row = enriched.iloc[-1]
    if passes_filters(row, analysis):
        atr = row["ATR"]
        entry = row["close"]
        stop = entry - 2.0 * atr
        tp = entry + 4.0 * atr
        account = trading_client.get_account()
        portfolio_value = float(account.equity)
        size = floor(config.defaults["risk_pct"] * portfolio_value / (entry - stop))
        candidate = {
            "symbol": symbol,
            "entry": entry,
            "stop": stop,
            "tp": tp,
            "size": size,
            "analysis": analysis,
        }
        logger.info(
            "Candidate %s size=%d entry=%.2f stop=%.2f tp=%.2f",
            symbol,
            size,
            entry,
            stop,
            tp,
        )
        await candidate_queue.put(candidate)


async def main():
    analysis_map: Dict[str, Dict] = {}
    while True:
        logger.debug(
            "analysis_queue=%d bar_queue=%d", analysis_queue.qsize(), bar_queue.qsize()
        )
        done = False

        while not analysis_queue.empty():
            data = await analysis_queue.get()
            analysis_map[data["key"]] = data
        while not bar_queue.empty():
            item = await bar_queue.get()
            sym = item["symbol"]
            bars = item["bars"]
            # Assume last news item for symbol
            for analysis in analysis_map.values():
                if sym in analysis.get("tickers", [sym]):
                    await process(sym, bars, analysis)
                    done = True
        if not done:
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
