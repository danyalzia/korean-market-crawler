# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.monostereo.app import PlaywrightBrowser, is_bad_link


async def test_bad_links(browser: PlaywrightBrowser):
    urls = {
        "https://b2b.monostereo1stop.com/international-noise-conspiracy-evp-098787055825.html",
        "https://b2b.monostereo1stop.com/ariana-grande-sweetener-import-2-lp-s-evp-602577005954.html",
        "https://b2b.monostereo1stop.com/supersuckers-tenderloin-split-evp-098787038477.html",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    bad_link = await is_bad_link(page)

    print(f"({url}) {bad_link}")

    await page.close()
