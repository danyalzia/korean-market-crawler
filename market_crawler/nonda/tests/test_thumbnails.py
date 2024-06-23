# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.nonda.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_images,
    parse_document,
    visit_link,
)


async def test_thumbnails(browser_headed: PlaywrightBrowser):
    urls = {
        "http://nonda.co.kr/product/detail.html?product_no=1689&cate_no=114&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1522&cate_no=114&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1465&cate_no=105&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1588&cate_no=105&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1657&cate_no=105&display_group=1",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_images = (await extract_thumbnail_images(document, url)).unwrap()

    print(f"{thumbnail_images = }")

    await page.close()
