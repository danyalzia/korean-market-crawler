from __future__ import annotations

import asyncio

from market_crawler.ddooroom.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "https://ddooroom.com/product/detail.html?product_no=1504&cate_no=53&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1370&cate_no=53&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1371&cate_no=53&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1530&cate_no=42&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1552&cate_no=42&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1457&cate_no=44&display_group=1",
        "https://ddooroom.com/product/detail.html?product_no=1190&cate_no=43&display_group=1",
    }

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page)).unwrap()
    print(f"{options = }")

    for option in options:
        print(split_options_text(option).unwrap())

    await page.close()
