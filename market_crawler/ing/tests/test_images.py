# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ing.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://ingdome.co.kr/product/detail.html?product_no=497&cate_no=130&display_group=1"
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    html_source = await extract_images(page, url, "", "")

    print(f"{html_source = }")
    assert html_source

    await page.close()
