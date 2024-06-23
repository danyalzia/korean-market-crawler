# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.yongsung.app import (
    PlaywrightBrowser,
    extract_product_name,
    visit_link,
)


async def test_product_name(browser: PlaywrightBrowser):
    urls = {
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100138",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100118",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    product_name = await extract_product_name(page)

    print(f"{product_name = }")
    assert product_name

    await page.close()
