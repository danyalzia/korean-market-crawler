# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ing.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://ingdome.co.kr/product/detail.html?product_no=497&cate_no=130&display_group=1",
        "https://ingdome.co.kr/product/detail.html?product_no=947&cate_no=89&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    *_, price2 = await extract_table(page)
    options = (await extract_options(page)).unwrap()
    assert options

    for option in options:
        print(f"{(split_options_text(option, price2)).unwrap() = }")

    await page.close()
