# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.koviss.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser_login: PlaywrightBrowser):
    urls = [
        "https://www.kovissb2b.com/product/%EC%BD%94%EB%B9%84%EC%8A%A4b2b-%EA%B3%A8%ED%94%84-%ED%95%84%EB%93%9C%EC%9A%A9%ED%92%88-%EC%83%9D%ED%99%94%EB%B3%BC%EB%A7%88%EC%BB%A4-%EC%84%A0%EB%AC%BC%EC%84%B8%ED%8A%B8-gs7908-%ED%95%B8%EB%93%9C%EB%A9%94%EC%9D%B4%EB%93%9C-%EA%B0%80%EC%A3%BD%ED%99%80%EB%8D%94-%EB%B3%B4%EC%84%9D%ED%95%A8/2847/category/37/display/1/",
        "https://www.kovissb2b.com/product/%EC%BD%94%EB%B9%84%EC%8A%A4b2b-%EC%9E%AC%EB%AF%B8%EC%9E%88%EB%8A%94-%ED%8C%A8%EB%9F%AC%EB%94%94-%ED%95%B5%EC%9D%B8%EC%8B%B8-%EB%B3%BC%EB%A7%88%EC%BB%A4-bm512-%EC%9E%90%EC%84%9D-%ED%99%80%EB%8D%94%ED%98%95/2839/category/124/display/1/",
        "https://www.kovissb2b.com/product/%EC%BD%94%EB%B9%84%EC%8A%A4b2b-%EA%B3%A8%ED%94%84%EC%9A%A9%ED%92%88-%EC%84%A0%EB%AC%BC%EC%84%B8%ED%8A%B8-gs7307a%ED%83%80%EC%9E%85-b%ED%83%80%EC%9E%85/2568/category/53/display/1/",
    ]

    tasks = [extract(url, browser_login) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await visit_link(page, url)

    options = await extract_options(page, url)

    for option in options:
        option1, price2, option2 = split_options_text(option, 34300)
        print(f"{option1 = }")
        print(f"{price2 = }")
        print(f"{option2 = }")

    await page.close()
