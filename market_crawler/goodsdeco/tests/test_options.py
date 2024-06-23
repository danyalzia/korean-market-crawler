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
    extract_options,
    parse_document,
    split_options_text,
)


async def test_options(browser: PlaywrightBrowser):
    urls = [
        "https://www.goodsdeco.com/goods/goods_view.php?goodsNo=1000003476",
    ]

    tasks = [extract(url, browser) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await visit_link(page, url)
    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    options = await extract_options(document)

    for option in options:
        option1, price2, option2, option3 = split_options_text(option, 34300)
        print(f"{option1 = }")
        print(f"{price2 = }")
        print(f"{option2 = }")
        print(f"{option3 = }")

    await page.close()
