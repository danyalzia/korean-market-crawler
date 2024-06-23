# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.leadersdome.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_data,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://leadersdome.co.kr/product/lds-20117054컬러/804/category/66/display/1/"
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        _,
        _,
        _,
        _,
        _,
        _,
        price2,
        _,
        _,
        options,
        _,
    ) = await extract_data(page, document, url, "", "")

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, price2))

    await page.close()
