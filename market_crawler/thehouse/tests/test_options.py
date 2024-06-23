# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.thehouse.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_price2,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://thehouse-mall.com/goods/goods_view.php?goodsNo=1000001570",
        "http://thehouse-mall.com/goods/goods_view.php?goodsNo=1000001464",
        "http://thehouse-mall.com/goods/goods_view.php?goodsNo=1000001328",
        "http://thehouse-mall.com/goods/goods_view.php?goodsNo=1000001369",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price2 = (await extract_price2(document)).unwrap()
    options = (await extract_options(page)).unwrap()

    for option in options:
        print(split_options_text(option, price2))

    await page.close()
