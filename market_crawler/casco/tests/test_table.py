# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.casco.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
)


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://cascokorea.co.kr/product/041535-speedairo2blue/121/category/33/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document)).unwrap()
    print(f"{table = }")

    await page.close()
