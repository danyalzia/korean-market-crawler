# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.goodsdeco.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name,
    parse_document,
)


async def test_product_name(browser: PlaywrightBrowser):
    urls = [
        "https://www.goodsdeco.com/goods/goods_view.php?goodsNo=1000004337",
        "https://www.goodsdeco.com/goods/goods_view.php?goodsNo=1000004095",
        "https://www.goodsdeco.com/goods/goods_view.php?goodsNo=1000004480",
    ]

    tasks = [extract(url, browser) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await visit_link(page, url, wait_until="load")
    content = await page.content()

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_name = await extract_product_name(document)

    print(f"{product_name = }")

    await page.close()
