# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hdf.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_images,
    parse_document,
    visit_link,
)


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://shop.ihdf.co.kr/shop_goods/goods_view.htm?category=01010800&goods_idx=7790&goods_bu_id=",
        "https://shop.ihdf.co.kr/shop_goods/goods_view.htm?category=040B0500&goods_idx=8424&goods_bu_id=",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    html_source = await extract_images(document, url, "", "")

    print(f"{html_source = }")
    assert html_source

    await page.close()
