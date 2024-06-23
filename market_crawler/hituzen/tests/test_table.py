# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hituzen.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://hituzen0598.cafe24.com/product/detail.html?product_no=138&cate_no=24&display_group=1",
    }

    tasks = ((extract(url, browser)) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = (await extract_table(page)).unwrap()
    print(f"{table = }")

    await page.close()
