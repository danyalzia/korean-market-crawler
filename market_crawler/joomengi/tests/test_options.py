# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.joomengi.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://shop1.mjmarket.cafe24.com/product/detail.html?product_no=2965&cate_no=35&display_group=1",
        "http://shop1.mjmarket.cafe24.com/product/detail.html?product_no=3541&cate_no=28&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page)).unwrap()

    for option in options:
        print(f"{option = }")

    await page.close()
