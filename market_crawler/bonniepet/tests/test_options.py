# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.bonniepet.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000015996",
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000014511",
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000014771",
    }

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    options = (await extract_options(page)).unwrap()

    for option in options:
        print(split_options_text(option).unwrap())

    await page.close()
