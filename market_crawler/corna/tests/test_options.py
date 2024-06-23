# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.corna.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    parse_document,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://corna.co.kr/product/%EC%9E%A5%EB%AF%B8%EB%82%98%EC%97%BC%EA%B0%80%EC%9A%B45383/425/category/32/display/1/",
    }

    tasks = ((extract(url, browser)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    options = await extract_options(document, page)
    print(f"{options = }")

    await page.close()
