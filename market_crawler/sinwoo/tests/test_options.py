# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sinwoo.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://www.sinwoo.com/shop/detail.htm?brandcode=2201-0226&page=1&cat_code=48/57/&sort=default&rows=7&cat_search=&cat_code=48/57/&pick=",
        "https://www.sinwoo.com/shop/detail.htm?brandcode=2201-0208&page=1&cat_code=48/57/&sort=default&rows=7&cat_search=&cat_code=48/57/&pick=",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    options = (await extract_options(page)).unwrap()

    print(f"{options = }")

    for option1 in options:
        print(option1)

    await page.close()
