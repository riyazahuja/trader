import asyncio
import logging

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import OrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass

import config
from risk_manager import approved_queue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

trading_client = TradingClient(
    api_key=config.APCA_API_KEY_ID,
    secret_key=config.APCA_API_SECRET_KEY,
    paper=True,
)


async def submit(candidate):
    order = OrderRequest(
        symbol=candidate["symbol"],
        qty=candidate["size"],
        side=OrderSide.BUY,
        type="market",
        time_in_force=TimeInForce.DAY,
        order_class=OrderClass.BRACKET,
        stop_loss={"stop_price": candidate["stop"]},
        take_profit={"limit_price": candidate["tp"]},
    )
    try:
        trading_client.submit_order(order)
        logger.info("Submitted order for %s", candidate["symbol"])
    except Exception as exc:
        logger.error("Order error: %s", exc)


async def main():
    while True:
        candidate = await approved_queue.get()
        await submit(candidate)


if __name__ == "__main__":
    asyncio.run(main())
