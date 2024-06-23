# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hyperinc.app import PlaywrightBrowser, extract_html, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EC%96%B4%EB%93%9C%EB%B0%B4%EC%8A%A4-%EB%B3%B4%EC%96%B4%EC%84%B8%EB%AF%B8-%EB%A0%88%EB%93%9C-%EC%97%90%EC%96%B4%EB%82%98%ED%94%84%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EB%85%80%EA%B3%B5%EC%9A%A9/78/category/176/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    html = await extract_html(page, url, "", "")

    print(f"{html = }")

    await page.close()
