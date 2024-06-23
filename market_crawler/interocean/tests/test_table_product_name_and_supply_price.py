# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.interocean.app import PlaywrightBrowser, extract_table, visit_link


async def test_table_product_name_and_price2(browser: PlaywrightBrowser):
    urls = {
        "http://interocean.co.kr/product/detail.html?product_no=73&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=2587&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=38&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=2006&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=44&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=54&cate_no=32&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await visit_link(page, url)

    product_name, price2 = await extract_table(page)

    print(f"{product_name = }")
    print(f"{price2 = }")

    assert product_name
    assert price2

    await page.close()
