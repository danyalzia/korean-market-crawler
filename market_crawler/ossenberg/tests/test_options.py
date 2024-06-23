# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ossenberg.app import (
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
        "http://ossenberg.co.kr/product/detail.html?product_no=28&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=32&cate_no=204&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=69&cate_no=204&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=110&cate_no=257&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        _,
        _,
        _,
        _,
        _,
        price2,
    ) = (await extract_table(page)).unwrap()

    options = (await extract_options(document, page)).unwrap()

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, price2).unwrap())

    await page.close()
