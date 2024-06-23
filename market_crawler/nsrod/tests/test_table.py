# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.nsrod.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "https://www.nsrod.co.kr/goods/view?no=367",
        "https://www.nsrod.co.kr/goods/view?no=532",
        "https://www.nsrod.co.kr/goods/view?no=359",
        "https://www.nsrod.co.kr/goods/view?no=30",
        "https://www.nsrod.co.kr/goods/view?no=553",
        "https://www.nsrod.co.kr/goods/view?no=542",
        "https://www.nsrod.co.kr/goods/view?no=287",
        "https://www.nsrod.co.kr/goods/view?no=79",
        "https://www.nsrod.co.kr/goods/view?no=89",
        "https://www.nsrod.co.kr/goods/view?no=499",
        "https://www.nsrod.co.kr/goods/view?no=106",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    match await extract_table(page, await page.content(), url):
        case list(table_list):
            for table in table_list:
                print(f"{table.model_name = }")
                print(f"{table.price2 = }")

        case str(table_html):
            print(f"{table_html = }")

    await page.close()
