# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.yongsung.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100138",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100118",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = await extract_table(page)

    print(f"{table.product_code = }")
    print(f"{table.percent = }")
    print(f"{table.price2 = }")
    print(f"{table.price3 = }")
    print(f"{table.sold_out_text = }")
    print(f"{table.manufacturing_country = }")

    await page.close()
