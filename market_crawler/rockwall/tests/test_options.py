# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.rockwall.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = [
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000617",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000495",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000616",
    ]

    tasks = [extract(url, browser_headed) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = await extract_table(document, url)
    options = await extract_options(page)

    for option in options:
        if isinstance(table.price2, int):
            print((split_options_text(option, table.price2)).unwrap())

    await page.close()
