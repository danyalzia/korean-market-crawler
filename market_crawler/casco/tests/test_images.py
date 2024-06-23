# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.casco.app import PlaywrightBrowser, extract_images


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "http://cascokorea.co.kr/product/041535-speedairo2blue/121/category/33/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()
    print(f"{images = }")

    await page.close()
