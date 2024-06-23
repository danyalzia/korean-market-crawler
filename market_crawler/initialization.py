# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from dataclasses import dataclass
from datetime import timedelta
from typing import cast

import backoff

from aiofile import AIOFile, LineReader
from throttler import Throttler

from market_crawler.cache import async_diskcache
from market_crawler.error import InvalidURL, TimeoutException, backoff_hdlr
from market_crawler.log import success


@dataclass(slots=True, frozen=True)
class Category:
    name: str
    url: str


# ? We will not fetch the urls again within 24 hours and just use the cached response
# ? This is done so that the server doesn't get a lot of requests on subsequent runs
@backoff.on_exception(
    backoff.expo,
    (TimeoutException, InvalidURL),  # type: ignore
    max_tries=5,
    on_backoff=backoff_hdlr,  # type: ignore
)
@async_diskcache(ttl=timedelta(hours=24))
async def verify_url(url: str, name: str, rate_limit: int) -> tuple[str, str]:
    """
    Check if the URL is valid
    """
    import aiohttp

    # ? Ignore if the SSL certificiation is failed
    # ? See: https://github.com/aio-libs/aiohttp/issues/955
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        try:
            async with Throttler(rate_limit, period=1.0):
                async with session.get(url) as response:
                    try:
                        response.raise_for_status()
                    except aiohttp.ClientResponseError as err:
                        raise InvalidURL(name, url=url) from err
                    return url, name
        except TimeoutError as err:
            raise TimeoutException(
                f"Timeout occurred: {name} | {url}",
            ) from err
        except aiohttp.ClientConnectorError as err:
            raise TimeoutException(
                f"The semaphore timeout period has expired (i.e., request is rejected by the server due to a lot of concurrent requests): {name} | {url}",
            ) from err


async def get_categories(
    sitename: str,
    filename: str = "categories.txt",
    rate_limit: int = 5,
) -> list[Category]:
    """
    Reads category names and their urls in categories.txt (default) present inside the sitename's directory

    Category names and urls must be separated by ","
    """

    filepath = os.path.join(os.path.dirname(__file__), sitename, filename)
    if not await asyncio.to_thread(
        os.path.exists,
        filepath,
    ):
        raise FileNotFoundError(
            f"{filename} is not present in the directory ({filepath})"
        )

    async with AIOFile(
        filepath,
        "r",
        encoding="utf-8",
    ) as afp:
        categories = [
            Category(
                name=str(cast("str", line).split(", ")[0].strip()),
                url=str(cast("str", line).split(", ")[-1].strip()),
            )
            async for line in LineReader(afp)
        ]

    for url, name in await asyncio.gather(
        *(
            verify_url(category.url, category.name, rate_limit)
            for category in categories  # type: ignore
        ),
    ):
        success(f"Verified: <blue>{url} ({name})</>")

    return categories
