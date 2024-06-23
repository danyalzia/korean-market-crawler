# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.pettory.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "https://pettory.com/product/detail.html?product_no=8375&cate_no=26&display_group=1",
        "https://pettory.com/product/detail.html?product_no=12038&cate_no=26&display_group=1",
        "https://pettory.com/product/detail.html?product_no=12776&cate_no=26&display_group=1",
        "https://pettory.com/product/detail.html?product_no=3925&cate_no=26&display_group=1",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    images = (await extract_images(page, url, "", "")).unwrap()
    print(f"{images = }")

    await page.close()
