# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.bonniepet.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000015945",
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000016500",
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000016493",
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000014511",
    }

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = await extract_table(document, page, url)

    print(f"{table = }")

    await page.close()
