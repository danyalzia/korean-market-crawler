# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.realbag.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "https://realbag.kr/product/detail.html?product_no=798&cate_no=4&display_group=1",
        "https://realbag.kr/product/detail.html?product_no=561&cate_no=4&display_group=1%0A",
        "https://realbag.kr/product/detail.html?product_no=539&cate_no=4&display_group=1",
        "https://realbag.kr/product/detail.html?product_no=446&cate_no=39&display_group=1",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()
    print(images)

    await page.close()
