# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.nsrod.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_image,
    parse_document,
    visit_link,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "https://www.nsrod.co.kr/goods/view?no=367",
        "https://www.nsrod.co.kr/goods/view?no=532",
        "https://www.nsrod.co.kr/goods/view?no=359",
        "https://www.nsrod.co.kr/goods/view?no=30",
        "https://www.nsrod.co.kr/goods/view?no=553",
        "https://www.nsrod.co.kr/goods/view?no=542",
        "https://www.nsrod.co.kr/goods/view?no=462",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_image = (await extract_thumbnail_image(document, url)).unwrap()

    print(f"{thumbnail_image = }")

    await page.close()
