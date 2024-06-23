# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ddooroom.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://ddooroom.com/product/detail.html?product_no=1504&cate_no=53&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1370&cate_no=53&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1371&cate_no=53&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1530&cate_no=42&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1552&cate_no=42&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1457&cate_no=44&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document, url)).unwrap()

    print(f"{table = }")

    await page.close()
