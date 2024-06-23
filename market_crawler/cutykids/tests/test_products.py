# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.cutykids.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    get_products,
    parse_document,
    visit_link,
)


async def test_products(browser_headed: PlaywrightBrowser):
    urls = {"http://www.cutykids.com/main.php?ac_id=11"}

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    products = (await get_products(document)).unwrap()

    print(f"{products = }")

    await page.close()
