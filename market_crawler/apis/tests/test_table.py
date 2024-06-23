# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.apis.app import PlaywrightBrowser, extract_table


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "http://apis.co.kr/shop_item_list.php?ac_id=128&ai_id=9321",
        "http://apis.co.kr/shop_item_list.php?ac_id=116&ai_id=2643",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = (await extract_table(page, url)).unwrap()
    print(f"{table = }")

    await page.close()
