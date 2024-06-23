# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.tnd.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_title,
    parse_document,
    visit_link,
)


async def test_product_title(browser: PlaywrightBrowser):
    urls = {
        "https://tndmall.shop/product/%ED%94%84%EB%A6%AC%EB%A7%88%EA%B3%A8%ED%94%84-%EC%97%AC%EC%84%B1-%EB%A9%94%EC%89%AC-%ED%8F%AC%EC%BB%A4%EC%8A%A4w-%EC%99%80%EC%9D%B4%EC%96%B4-%EC%9A%B4%EB%8F%99%ED%99%94-maf0005/5657/category/189/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_title = await extract_product_title(document)
    print(f"{product_title = }")

    await page.close()
