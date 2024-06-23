# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hangnams.app import PlaywrightBrowser, extract_html, visit_link


async def test_html(browser: PlaywrightBrowser):
    urls = {
        "https://hangnams.com/goods/view?no=878",
        "https://hangnams.com/goods/view?no=777",
        "https://hangnams.com/goods/view?no=874",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    html = await extract_html(page, url, "", "")

    print(f"{html = }")

    await page.close()
