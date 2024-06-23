# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.leadersdome.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser: PlaywrightBrowser):
    urls = {
        "https://leadersdome.co.kr/product/ld-8033%EC%BB%AC%EB%9F%AC/1346/category/53/display/1/#sstab1",
        "https://leadersdome.co.kr/product/ld-7181%EC%BB%AC%EB%9F%AC/1179/category/28/display/1/",
        "https://leadersdome.co.kr/product/ld-6494%EC%BB%AC%EB%9F%AC/1086/category/28/display/1/",
        "https://leadersdome.co.kr/product/ld-6011%EC%BB%AC%EB%9F%AC/993/category/28/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (data) = await extract_data(page, document, url, "", "")

    print(f"{data = }")

    await page.close()
