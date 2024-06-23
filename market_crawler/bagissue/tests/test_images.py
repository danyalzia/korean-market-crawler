# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.bagissue.app import (
    PlaywrightBrowser,
    ensure_login,
    extract_images,
    visit_link,
)


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.bagissue.co.kr/product/%EC%BA%A3%ED%82%B7-a815/1694/category/70/display/1/",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    await ensure_login(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()
    print(images)

    await page.close()
