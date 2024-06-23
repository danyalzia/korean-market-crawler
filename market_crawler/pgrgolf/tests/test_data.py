# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.pgrgolf.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser: PlaywrightBrowser):
    urls = [
        "https://pgr.co.kr/product/pgr%EA%B3%A8%ED%94%84-%EC%97%AC%EC%84%B1%EA%B3%A8%ED%94%84-%EC%86%8C%EB%A7%A4%ED%8F%AC%EC%9D%B8%ED%8A%B8-%EB%B0%98%ED%8C%94%ED%8B%B0%EC%85%94%EC%B8%A0-gt-4287/1440/category/140/display/1/",
    ]

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)
    data = await extract_data(page, document, url, "", "")

    print(f"{data = }")

    await page.close()
