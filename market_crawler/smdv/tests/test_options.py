# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.smdv.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "https://smdv.kr/product/detail.html?product_no=534&cate_no=1&display_group=37",
        "https://smdv.kr/product/detail.html?product_no=1583&cate_no=1&display_group=50",
    }

    tasks = (extract(url, browser_headed) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    _, price2, _ = (await extract_table(document)).unwrap()
    options_list = await extract_options(document)

    for option in options_list:
        if isinstance(price2, int):
            print(split_options_text(option, price2).unwrap())

    await page.close()
