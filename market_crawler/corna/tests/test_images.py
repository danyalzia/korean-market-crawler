# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.corna.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://corna.co.kr/product/%EC%9E%A5%EB%AF%B8%EB%82%98%EC%97%BC%EA%B0%80%EC%9A%B45383/425/category/32/display/1/",
    }

    tasks = ((extract(url, browser)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    html_source = (await extract_images(page, url, "", "")).unwrap()
    print(f"{html_source = }")

    await page.close()
