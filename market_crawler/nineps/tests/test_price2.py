# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.nineps.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_price2,
    extract_table,
    parse_document,
    visit_link,
)


async def test_price2(browser: PlaywrightBrowser):
    urls = {
        "http://9ps.kr/product/view.php?id=33630&cid=7",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price2 = (await extract_price2(document)).unwrap()
    table = (await extract_table(document)).unwrap()

    print(f"{price2 = }")
    print(f"{table = }")

    await page.close()
