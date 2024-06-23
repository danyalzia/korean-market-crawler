# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.rockwall.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = [
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000617",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000660",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000581",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000531",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000495",
    ]

    tasks = [extract(url, browser) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = await extract_table(document, url)

    print(f"{table = }")

    await page.close()
