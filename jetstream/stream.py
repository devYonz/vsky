import asyncio
from time import sleep
from typing import List
from IPython.core.interactiveshell import InteractiveShell

InteractiveShell.ast_node_interactivity = "all"  # type: ignore

import logging
import os
import json

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    datefmt="%I:%M:%S",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def l(msg):
    logger.info(msg)


def ld(msg):
    logger.debug(msg)

from atproto import AsyncClient
from dotenv import load_dotenv

# Load the .env file
load_dotenv("./yonz.env")

# Access the values
LOGIN_PASS = os.getenv('LOGIN_PASS')

async def start_stream(client: AsyncClient, pages=20):
    l("Starting stream")
    try:
        data = await client.get_timeline(limit=30)
    except Exception as e:
        logger.error(f"Error fetching timeline: {e}")
        return []

    feed = data.feed
    next_page = data.cursor
    count = 0
    while next_page and count < pages:
        try:
            data = await client.get_timeline(cursor=next_page, limit=30)
        except Exception as e:
            logger.error(f"Error fetching timeline page {count}: {e}")
            break
        next_page = data.cursor
        feed.extend(data.feed)
        ld(f"Next page: {next_page}")
        count += 1
    return feed

async def main():
    client = AsyncClient()
    await client.login("yonz.bsky.social", LOGIN_PASS)
    candidate_list = await start_stream(client)
    import pandas as pd

    # Convert candidate_list to a pandas DataFrame
    df = pd.DataFrame([item.model_dump() for item in candidate_list])

    # Ensure the directory exists
    os.makedirs('z', exist_ok=True)
    # Write DataFrame to parquet file
    df.to_parquet('z/bsky_feed.parquet')

if __name__ == "__main__":
    asyncio.run(main())