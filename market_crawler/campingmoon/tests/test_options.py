# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.campingmoon.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "https://campingmoon.co.kr/product/detail.html?product_no=1599&cate_no=152&display_group=1",
    }

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    try:
        table = (await extract_table(document)).unwrap()
    except Exception:
        print(url)
        raise

    options = (await extract_options(page)).unwrap()
    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, table.price3).unwrap())

    await page.close()
