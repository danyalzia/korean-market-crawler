# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.yongsung.app import (
    PlaywrightBrowser,
    extract_thumbnail_image,
    visit_link,
)


async def test_thumbnail(browser_headed: PlaywrightBrowser):
    urls = {
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100138",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100118",
    }

    tasks = (extract(url, browser_headed) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    thumbnail_image = await extract_thumbnail_image(page)

    print(f"{thumbnail_image = }")
    assert thumbnail_image

    await page.close()
