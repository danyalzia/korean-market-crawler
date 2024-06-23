# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.purefishing import config
from market_crawler.purefishing.app import (
    Category,
    HTMLParsingError,
    PlaywrightBrowser,
    get_categories,
    get_products,
    parse_document,
    visit_link,
)


async def test_products(browser_headed: PlaywrightBrowser):
    subcategories = await get_categories(
        sitename=config.SITENAME, filename="subcategories.txt"
    )

    tasks = (extract(category, browser_headed) for category in subcategories)
    await asyncio.gather(*tasks)


async def extract(category: Category, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, category.url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=category.url)

    try:
        products = (await get_products(document)).unwrap()
    except Exception:
        print(f"Products not found: {category.url}")
        await page.close()
        return

    print(f"Products: {len(products)} ({category.url})")

    await page.close()
