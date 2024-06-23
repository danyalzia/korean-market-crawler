# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hyperinc.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name,
    parse_document,
    visit_link,
)


async def test_title(browser: PlaywrightBrowser):
    urls = {
        "https://hyperinc.kr/product/gravitor%EA%B7%B8%EB%9D%BC%EB%B9%84%ED%84%B0-%EC%9E%90%EC%BC%93%EC%8A%88%ED%8A%B8-2mm/611/category/175/display/1/"
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_name = (await extract_product_name(document)).unwrap()

    print(f"{product_name = }")

    assert product_name

    await page.close()
