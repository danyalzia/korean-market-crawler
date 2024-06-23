# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.bagissue.app import (
    PlaywrightBrowser,
    ensure_login,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.bagissue.co.kr/product/%EC%BA%A3%ED%82%B7-a815/1694/category/70/display/1/",
        "http://bagissue.co.kr/product/detail.html?product_no=1569&cate_no=12&display_group=1",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    await ensure_login(page, url)

    options = (await extract_options(page, url)).unwrap()

    for option in options:
        print(split_options_text(option).unwrap())

    await page.close()
