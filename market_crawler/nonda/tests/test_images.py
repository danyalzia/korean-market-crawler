# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.nonda.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "http://nonda.co.kr/product/detail.html?product_no=1689&cate_no=114&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1522&cate_no=114&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1465&cate_no=105&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1588&cate_no=105&display_group=1",
        "http://nonda.co.kr/product/detail.html?product_no=1657&cate_no=105&display_group=1",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()

    print(f"{images = }")

    await page.close()
