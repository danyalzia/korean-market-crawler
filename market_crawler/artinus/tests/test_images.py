# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.artinus.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "http://partner.artinus.net/partner/?inc=detail&pcode=AR-964_NAVY",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AR-964_RED",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AR-966_RED",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AV-150",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AT-599R",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AT-597W",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    html_source = (await extract_images(page, url, "", "")).unwrap()

    print(f"{html_source = }")

    await page.close()
