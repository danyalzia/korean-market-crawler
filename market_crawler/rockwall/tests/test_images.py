# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.rockwall.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
    urls = [
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000617",
        "https://www.rockwall.co.kr/goods/goods_view.php?goodsNo=1000000671",
    ]

    tasks = [extract(url, browser_headed) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    detailed_images_html_source = await extract_images(page, url, "", "")
    assert detailed_images_html_source.ok()

    print(f"{detailed_images_html_source = }")

    await page.close()
