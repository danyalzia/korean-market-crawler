# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.landas.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_images,
    parse_document,
    visit_link,
)


async def test_thumbnails(browser: PlaywrightBrowser):
    urls = {
        "https://www.ldsb2bmall.com/shop/goods/goods_view.php?goodsno=335&category=001007",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_images = (await extract_thumbnail_images(document, url)).unwrap()

    print(f"{thumbnail_images = }")

    await page.close()
