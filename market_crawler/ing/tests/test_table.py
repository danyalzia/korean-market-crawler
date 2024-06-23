# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ing.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "https://ingdome.co.kr/product/detail.html?product_no=497&cate_no=130&display_group=1",
        "https://ingdome.co.kr/product/detail.html?product_no=2646&cate_no=130&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    manufacturer, manufacturing_country, quantity, price2 = await extract_table(page)

    assert manufacturer
    if (
        url
        != "https://ingdome.co.kr/product/detail.html?product_no=2646&cate_no=130&display_group=1"
    ):
        assert manufacturing_country
    assert quantity
    assert price2

    print(f"{manufacturer = }")
    print(f"{manufacturing_country = }")
    print(f"{quantity = }")
    print(f"{price2 = }")

    await page.close()
