# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.roomandoffice.app import (
    PlaywrightBrowser,
    extract_images,
    visit_link,
)


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://xn--jt2by0pl8b7va956c.kr/product/ywd4009-ok/461/category/246/display/1",
        "https://xn--jt2by0pl8b7va956c.kr/product/로나-단스탠드/1986/category/27/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()

    print(f"{images = }")

    await page.close()
