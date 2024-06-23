# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.manatee.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://rain119.co.kr/goods/goods_view.php?goodsNo=1000000862",
        "http://rain119.co.kr/goods/goods_view.php?goodsNo=1000000872",
        "http://rain119.co.kr/goods/goods_view.php?goodsNo=1000000871",
        "http://rain119.co.kr/goods/goods_view.php?goodsNo=1000000881",
        "http://rain119.co.kr/goods/goods_view.php?goodsNo=1000000002",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page)).unwrap()

    print(f"{options = }")

    await page.close()
