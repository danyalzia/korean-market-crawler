# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.xeeon.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_image,
    parse_document,
    visit_link,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "https://xeeon.co.kr/product/detail.html?product_no=2388&cate_no=72&display_group=1",
        "http://xeeon.co.kr/product/detail.html?product_no=553&cate_no=72&display_group=1",
        "https://xeeon.co.kr/product/detail.html?product_no=2228&cate_no=157&display_group=1",
        "https://xeeon.co.kr/product/detail.html?product_no=2231&cate_no=157&display_group=1",
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
