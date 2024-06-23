# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.monostereo.app import PlaywrightBrowser, extract_text


async def test_text(browser: PlaywrightBrowser):
    urls = {
        "https://b2b.monostereo1stop.com/billie-eilish-happier-than-ever-2-lp-evp-602435973548.html",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    product_detailed_text = (await extract_text(page)).unwrap()

    print(f"product_detailed_text = {product_detailed_text}")

    await page.close()
