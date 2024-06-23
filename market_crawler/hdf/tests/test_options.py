# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hdf.app import PlaywrightBrowser, extract_options2, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://shop.ihdf.co.kr/shop_goods/goods_view.htm?category=01010600&goods_idx=7746&goods_bu_id=",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = await extract_options2(page)

    print(f"{options = }")
    assert options

    await page.close()
