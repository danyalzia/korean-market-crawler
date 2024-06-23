# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.geosang.app import (
    PlaywrightBrowser,
    extract_options,
    extract_price2,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=20528&category=074007",
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=16316&category=012016",
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=15583&category=012016",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    document = await parse_document(await page.content(), engine="lxml")
    assert document

    price2 = (await extract_price2(document)).unwrap()

    options = await extract_options(document)

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, price2).unwrap())

    await page.close()
