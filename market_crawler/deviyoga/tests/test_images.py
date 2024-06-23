# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.deviyoga.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000246",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000004",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000011",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()
    print(images)

    await page.close()
