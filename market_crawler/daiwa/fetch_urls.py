# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import sys

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count, freeze_support
from typing import Any

import aiohttp
import pandas as pd

from market_crawler import log
from market_crawler.cache import async_diskcache
from market_crawler.helpers import chunks


sys.path.insert(0, "..")
sys.path.insert(0, "../..")


TOTAL_PAGES = 15
CHUNK_SIZE = 5


@async_diskcache(ttl="24h")
async def fetch(url: str):
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,ko;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "x-requested-with": "XMLHttpRequest",
        "cookie": "JSESSIONID=7A8679633F0EAC516F2599E07973EB6E",
        "Referer": "http://www.daiwab2b.com/sencha/list",
        "Referrer-Policy": "strict-origin-when-cross-origin",
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            return await response.json()


def construct_url(page_no: int):
    return (
        f"http://www.daiwab2b.com/model/girdItemList?_dc=1646027372316&keyword=&clsS=&clsSSub=&price=&isPaging=true&stockType=M&page={page_no}&start=0&limit=100"
        if page_no == 1
        else f"http://www.daiwab2b.com/model/girdItemList?_dc=1646027372316&keyword=&clsS=&clsSSub=&price=&isPaging=true&stockType=M&page={page_no}&start={(page_no-1)*100}&limit=100"
    )


async def fetch_pages(page_no: int):
    url = construct_url(page_no)
    log.info(f"Fetching: {url}")

    return await fetch(url)


async def main():
    os.makedirs("urls", exist_ok=True)

    for chunk in chunks(range(1, TOTAL_PAGES + 1), CHUNK_SIZE):
        tasks = (fetch_pages(page_no) for page_no in chunk)
        responses = await asyncio.gather(*tasks)

        with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
            results = [
                executor.submit(to_excel, page_no, response)
                for page_no, response in zip(chunk, responses, strict=True)
            ]

        for r in results:
            r.result()


def to_excel(page_no: int, response: dict[str, Any]):
    df = pd.DataFrame(response["list"])
    path = os.path.join("urls", f"{page_no}.xlsx")
    df.to_excel(path, index=False)
    log.info(f"Saved: {path}")


if __name__ == "__main__":
    freeze_support()
    asyncio.run(main())
