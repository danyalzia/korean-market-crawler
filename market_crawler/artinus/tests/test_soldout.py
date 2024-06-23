# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.artinus.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_soldout_text,
    get_products,
    parse_document,
    visit_link,
)


async def test_soldout(browser: PlaywrightBrowser):
    urls = {
        "http://partner.artinus.net/partner/?page=7&order=&by=&num_per_page=&mod=&actpage=&searchval=&cate=035002&inc=list"
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    products = (await get_products(document)).unwrap()
    products1_len = len(products)
    assert products1_len

    print(f"{products1_len = }")

    for idx in range(products1_len):
        soldout_text = await extract_soldout_text(products[idx])
        print(f"{soldout_text = }")

    await page.close()
