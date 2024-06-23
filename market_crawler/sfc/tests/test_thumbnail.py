# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sfc.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_image,
    get_products,
    parse_document,
    visit_link,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "http://www.xn--9t4b29bmob475q.com/goods/goods_list.php?cateCd=005",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    products = (await get_products(document)).unwrap()

    for idx in range(len(products)):
        thumbnail_image = (await extract_thumbnail_image(products[idx], url)).unwrap()

        print(f"{thumbnail_image = }")
        assert thumbnail_image

    await page.close()
