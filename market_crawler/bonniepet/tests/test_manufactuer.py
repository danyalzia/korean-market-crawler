# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.bonniepet.app import (
    PlaywrightBrowser,
    extract_manufacturer,
    visit_link,
)


async def test_manufacturer(browser: PlaywrightBrowser):
    urls = {
        "http://www.bonniepet.co.kr/goods/goods_view.php?goodsNo=1000016582",
    }

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    manufacturer = await extract_manufacturer(page)
    assert manufacturer.ok()

    print(f"{manufacturer = }")
    await page.close()
