# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.thehouse.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_message1,
    parse_document,
    visit_link,
)


async def test_paragraphs(browser: PlaywrightBrowser):
    urls = {
        "http://thehouse-mall.com/goods/goods_view.php?goodsNo=1000001090",
        "http://thehouse-mall.com/goods/goods_view.php?goodsNo=1000000720",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    paragraphs = (await extract_message1(document)).ok()
    print(f"{paragraphs = }")

    await page.close()
