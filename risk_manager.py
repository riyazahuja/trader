import asyncio
import logging
from typing import Dict, List

from alpaca.trading.client import TradingClient

import config
from selector import candidate_queue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

approved_queue: asyncio.Queue = asyncio.Queue()

trading_client = TradingClient(
    api_key=config.APCA_API_KEY_ID,
    secret_key=config.APCA_API_SECRET_KEY,
    paper=True,
)


async def current_positions() -> List[Dict]:
    positions = trading_client.get_all_positions()
    return [p.__dict__ for p in positions]


def total_at_risk(positions: List[Dict]) -> float:
    total = 0.0
    for p in positions:
        total += float(p.get("unrealized_plpc", 0)) * float(p.get("market_value", 0))
    return abs(total)


async def process(candidate: Dict):
    positions = await current_positions()
    risk = total_at_risk(positions)
    account = trading_client.get_account()
    portfolio_value = float(account.equity)
    new_trade_loss = (candidate["entry"] - candidate["stop"]) * candidate["size"]
    if risk + new_trade_loss > config.defaults["max_total_risk"] * portfolio_value:
        logger.info("Risk limit reached; rejecting trade for %s", candidate["symbol"])
        return
    if len(positions) >= config.defaults["max_open_trades"]:
        logger.info("Max open trades reached; rejecting trade")
        return
    await approved_queue.put(candidate)


async def main():
    while True:
        candidate = await candidate_queue.get()
        await process(candidate)


if __name__ == "__main__":
    asyncio.run(main())
