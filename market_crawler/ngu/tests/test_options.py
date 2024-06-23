# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ngu.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = [
        "https://ngub2b.com/product/%EA%B3%A0%EC%A0%95%ED%95%80-%EB%AC%B4%EB%A3%8C-%EC%BD%94%EC%BD%94%ED%8C%9C-%EC%95%BC%EC%9E%90%EB%A7%A4%ED%8A%B8-%ED%94%84%EB%A6%AC%EB%AF%B8%EC%97%84-%EC%95%BC%EC%9E%90%EB%A7%A4%ED%8A%B8-%ED%8F%AD-06m-20m-%EA%B8%B8%20%EC%9D%B4-10m/167/category/25/display/1/",
        "https://ngub2b.com/product/%EA%B3%A0%EC%A0%95%ED%95%80-%EB%AC%B4%EB%A3%8C-%EC%BD%94%EC%BD%94%ED%8C%9C-%EC%95%BC%EC%9E%90%EB%A7%A4%ED%8A%B8-%ED%94%84%EB%A6%AC%EB%AF%B8%EC%97%84-%EC%95%BC%EC%9E%90%EB%A7%A4%ED%8A%B8-%ED%8F%AD-06m-20m-%EA%B8%B8%EC%9D%B4-10m/167/category/25/display/1/",
    ]

    tasks = [extract(url, browser) for url in set(urls)]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = await extract_options(page)
    print(f"{options = }")

    await page.close()
