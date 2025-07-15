# run_all.py
import asyncio
from ingest import fetch_news, fetch_bars
from nlp_engine import main as analyze_loop  # wrap your existing main() as a coroutine
from selector import main as selector_loop
from risk_manager import main as risk_loop
from executor import main as executor_loop
from monitor import poll

import config


async def main():
    tasks = [
        asyncio.create_task(fetch_news()),
        *[asyncio.create_task(fetch_bars(sym)) for sym in config.WATCHLIST],
        asyncio.create_task(analyze_loop()),
        asyncio.create_task(selector_loop()),
        asyncio.create_task(risk_loop()),
        asyncio.create_task(executor_loop()),
        asyncio.create_task(poll()),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
