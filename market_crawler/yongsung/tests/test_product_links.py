# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.yongsung import config
from market_crawler.yongsung.app import (
    Category,
    HTMLParsingError,
    PlaywrightBrowser,
    get_categories,
    get_products,
    parse_document,
    visit_link,
)


async def test_links(browser_headed: PlaywrightBrowser):
    categories = await get_categories(sitename=config.SITENAME)

    tasks = (extract(url, browser_headed) for url in categories[:2])
    await asyncio.gather(*tasks)


async def extract(category: Category, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, category.url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=category.url)

    products = (await get_products(document)).unwrap()
    products1_len = len(products)
    assert products1_len

    await page.close()
