# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.murray.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://murray.co.kr/goods/goods_view.php?goodsNo=1000000164",
        "http://murray.co.kr/goods/goods_view.php?goodsNo=1000000183",
        "http://murray.co.kr/goods/goods_view.php?goodsNo=1000000172",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    document = await parse_document(await page.content(), engine="lxml")
    assert document

    *_, price2 = (await extract_table(document)).unwrap()
    options = (await extract_options(page)).unwrap()

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, price2))

    await page.close()
