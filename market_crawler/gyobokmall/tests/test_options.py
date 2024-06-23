# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.gyobokmall.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "http://gbmb2b.com/product/detail.html?product_no=10653&cate_no=76&display_group=1",
        "http://gbmb2b.com/product/detail.html?product_no=38108&cate_no=75&display_group=1",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    _, price3, *_ = (await extract_table(page)).unwrap()
    options = (await extract_options(page, url)).unwrap()

    for option in options:
        print(split_options_text(option, price3).unwrap())

    await page.close()
