# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.safetec.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://safetecb2b.co.kr/product/%EB%AA%BD%ED%81%AC%EB%A1%9C%EC%8A%A4-mc-71w%EB%B0%A9%ED%95%9C%ED%99%94/465/category/74/display/2/",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page)).unwrap()
    print(f"{options = }")

    await page.close()
