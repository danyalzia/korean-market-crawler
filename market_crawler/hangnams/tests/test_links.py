# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hangnams.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    get_product_link,
    get_products,
    parse_document,
    visit_link,
)


async def test_links(browser: PlaywrightBrowser):
    urls = {
        "https://hangnams.com/goods/catalog?code=0023",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    products = (await get_products(document)).unwrap()

    for product in products:
        print((await get_product_link(product)).unwrap())

    await page.close()
