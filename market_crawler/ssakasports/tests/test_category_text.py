# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ssakasports.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_category_text,
    parse_document,
    visit_link,
)


async def test_category_text(browser: PlaywrightBrowser):
    urls = {
        "https://www.ssakasports.co.kr/product/pro_view?id=69414&flag=&autoplay=0",
        "https://www.ssakasports.co.kr/product/pro_view?id=60462&flag=&autoplay=0",
        "https://www.ssakasports.co.kr/product/pro_view?id=71808&flag=&autoplay=0",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    category_text = (await extract_category_text(document)).unwrap()

    print(f"{category_text = }")

    await page.close()
