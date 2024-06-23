# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.bnkrod.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = [
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000128",
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000004",
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000070",
    ]

    tasks = [extract(url, browser) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = await extract_table(page, url)
    print(f"{table = }")
