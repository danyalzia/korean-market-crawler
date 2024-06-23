from __future__ import annotations

import asyncio

from market_crawler.franklinsports.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "http://franklinsports.co.kr/product/detail.html?product_no=607&cate_no=168&display_group=1#none",
        "http://franklinsports.co.kr/product/detail.html?product_no=368&cate_no=65&display_group=1#none",
    }

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options, options4 = (await extract_options(page)).unwrap()
    print(f"{options = }")
    print(f"{options4 = }")

    for option in options:
        print(split_options_text(option).unwrap())

    await page.close()
