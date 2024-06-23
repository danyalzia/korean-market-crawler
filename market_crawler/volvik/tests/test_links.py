# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.volvik.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    get_product_link,
    get_products,
    parse_document,
    visit_link,
)


async def test_links(browser: PlaywrightBrowser):
    urls = {
        "https://www.volvik.co.kr/products/golf/balls/1",
        "https://www.volvik.co.kr/products/golf/balls/2",
        "https://www.volvik.co.kr/products/golf/balls/3",
        "https://www.volvik.co.kr/products/golf/balls/4",
    }

    tasks = (
        extract(url, page_no, browser) for page_no, url in enumerate(urls, start=1)
    )
    await asyncio.gather(*tasks)


async def extract(url: str, page_no: int, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    products = (await get_products(document)).unwrap()

    for product in products:
        print(await get_product_link(product, page_no))

    await page.close()
