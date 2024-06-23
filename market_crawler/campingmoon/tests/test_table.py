# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.campingmoon.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "https://campingmoon.co.kr/product/detail.html?product_no=1599&cate_no=152&display_group=1",
    }

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = await extract_table(document)
    print(f"{table = }")

    await page.close()
