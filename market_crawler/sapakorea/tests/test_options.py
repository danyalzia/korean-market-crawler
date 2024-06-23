# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sapakorea.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://sapakorea.co.kr/product/detail.html?product_no=989&cate_no=100&display_group=1",
        "http://sapakorea.co.kr/product/detail.html?product_no=123&cate_no=100&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=834&cate_no=100&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1236&cate_no=160&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document, url)).expect(url)
    options = (await extract_options(page)).unwrap()

    for option in options:
        print(f"{option = }")
        if isinstance(table.price2, int):
            print(f"{split_options_text(option, table.price2)}")

    await page.close()
