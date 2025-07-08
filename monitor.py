import asyncio
import csv
import logging
from datetime import datetime

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus

import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

trading_client = TradingClient(
    api_key=config.APCA_API_KEY_ID,
    secret_key=config.APCA_API_SECRET_KEY,
    paper=True,
)


async def poll():
    while True:

        request_params = GetOrdersRequest(
            status=QueryOrderStatus.ALL, limit=50  # OPEN | CLOSED | ALL
        )
        orders = trading_client.get_orders(filter=request_params)
        logger.info("Polled %d orders", len(orders))
        with open("trades.csv", "a", newline="") as f:
            writer = csv.writer(f)
            for order in orders:
                writer.writerow(
                    [
                        datetime.utcnow().isoformat(),
                        order.symbol,
                        order.filled_qty,
                        order.filled_avg_price,
                        order.status,
                    ]
                )
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(poll())
