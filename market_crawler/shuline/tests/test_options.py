# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.shuline.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://www.shuline.co.kr/product/detail.html?product_no=63842&cate_no=391&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=67341&cate_no=43&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=65927&cate_no=43&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=65095&cate_no=43&display_group=1",
        "http://www.shuline.co.kr/product/detail.html?product_no=65736&cate_no=57&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    content = await page.content()

    table = (await extract_table(content)).expect(f"{url}")
    options = (await extract_options(page, page)).unwrap()

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, table.price2).unwrap())

    await page.close()
