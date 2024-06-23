# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.deviyoga.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser_headed: PlaywrightBrowser):
    urls = {
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000246",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000004",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000276",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000330",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document, url)).unwrap()

    print(f"{table = }")

    await page.close()
