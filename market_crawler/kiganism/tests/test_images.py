# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.kiganism.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000181",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000182",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000706",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000752",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000750",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    images = (await extract_images(page, url)).unwrap()
    print(f"{images = }")

    await page.close()
