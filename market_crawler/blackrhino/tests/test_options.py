# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.blackrhino.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3329&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3330&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3328&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3327&category=041",
    }

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    table = (await extract_table(page)).unwrap()

    options = (await extract_options(page)).unwrap()
    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, table.price3).unwrap())

    await page.close()
