# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sdf.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://badmintonworld.co.kr/product/maxx-%ED%86%A0%EB%84%A4%EC%9D%B4%EB%8F%84-%EC%9A%B0%EB%B8%90-m2/168/category/58/display/1/",
        "https://badmintonworld.co.kr/product/maxx-%ED%86%A0%EB%84%A4%EC%9D%B4%EB%8F%84-%EC%9A%B0%EB%B8%90-m3/167/category/58/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page)).unwrap()
    assert options

    for option in options:
        print(f"{option = }")

    await page.close()
