# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.monostereo.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name,
    parse_document,
)


async def test_product_name(browser: PlaywrightBrowser):
    urls = {
        "https://b2b.monostereo1stop.com/billie-eilish-happier-than-ever-2-lp-evp-602435973548.html",
        "https://b2b.monostereo1stop.com/tom-petty-the-heartbreakers-live-at-the-fillmore-1997-3-lp-s-evp-093624882602.html",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_name = (await extract_product_name(document)).unwrap()

    print(f"product_name = {product_name}")

    await page.close()
