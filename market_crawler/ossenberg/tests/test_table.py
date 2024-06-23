# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ossenberg.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://ossenberg.co.kr/product/detail.html?product_no=36&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=28&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=45&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=119&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=32&cate_no=204&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=69&cate_no=204&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=152&cate_no=50&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=42&cate_no=214&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=98&cate_no=204&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    (
        product_name,
        model_name,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
    ) = (await extract_table(page)).unwrap()

    print(f"{product_name = }")
    print(f"{model_name = }")
    print(f"{manufacturer = }")
    print(f"{manufacturing_country = }")
    print(f"{price3 = }")
    print(f"{price2 = }")

    await page.close()
