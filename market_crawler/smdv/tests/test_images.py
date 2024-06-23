# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.smdv.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "https://smdv.kr/product/detail.html?product_no=1607&cate_no=1&display_group=8",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    images = (await extract_images(page, url, "", "")).unwrap()
    print(f"{images = }")

    await page.close()
