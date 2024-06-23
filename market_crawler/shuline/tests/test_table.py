# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.shuline.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://www.shuline.co.kr/product/detail.html?product_no=65465&cate_no=28&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=49031&cate_no=359&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=67341&cate_no=43&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=65927&cate_no=43&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=65095&cate_no=43&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=65736&cate_no=57&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    content = await page.content()

    table = (await extract_table(content)).unwrap()

    print(f"{table = }")

    await page.close()
