# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domeplay.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://domeplay.co.kr/product/detail.html?product_no=2471&cate_no=1&display_group=5",
        "https://domeplay.co.kr/product/detail.html?product_no=4497&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4496&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4495&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4494&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4428&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4433&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4492&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4493&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4423&cate_no=315&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    price, _, _ = await extract_table(page)

    options = (await extract_options(page)).unwrap()

    print(f"{options = }")

    for option in options:
        print(split_options_text(option, price))

    await page.close()
