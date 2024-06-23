# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.funnydome.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://funnydome.com/product/detail.html?product_no=382&cate_no=61&display_group=1",
        "https://funnydome.com/product/detail.html?product_no=187&cate_no=61&display_group=1",
        "https://funnydome.com/product/detail.html?product_no=188&cate_no=61&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    document = await parse_document(await page.content(), engine="lxml")
    assert document

    price2, *_ = (await extract_table(document)).unwrap()
    options = (await extract_options(page)).unwrap()

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, price2))

    await page.close()
