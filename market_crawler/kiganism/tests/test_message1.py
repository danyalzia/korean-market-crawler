# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.kiganism.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_message1,
    parse_document,
)


async def test_message1(browser: PlaywrightBrowser):
    urls = {
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000153",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000181",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000704",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    message1 = (await extract_message1(document)).unwrap()

    print(message1)

    await page.close()
