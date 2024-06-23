# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.scubapro.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_images,
    parse_document,
    visit_link,
)


async def test_thumbnails(browser: PlaywrightBrowser):
    urls = {
        "https://www.scubapro.co.kr/product/read.jsp?reqPageNo=1&sdepth1=%ED%98%B8%ED%9D%A1%EA%B8%B0&no=427",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_images = (await extract_thumbnail_images(document, url)).unwrap()

    print(f"{thumbnail_images = }")

    await page.close()
