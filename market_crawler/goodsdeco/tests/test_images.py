# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.goodsdeco.app import PlaywrightBrowser, extract_images


async def test_images(browser: PlaywrightBrowser):
    urls = [
        "https://www.goodsdeco.com/goods/goods_view.php?goodsNo=1000004337",
    ]

    tasks = [extract(url, browser) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    detailed_images_html_source = (await extract_images(page, url, "", "")).unwrap()

    print(f"{detailed_images_html_source = }")

    await page.close()
