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
    extract_message2,
    parse_document,
)


async def test_message2(browser: PlaywrightBrowser):
    urls = {
        "https://b2b.monostereo1stop.com/billie-eilish-happier-than-ever-2-lp-evp-602435973548.html",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    message2 = (await extract_message2(document)).unwrap()

    print(f"message2 = {message2}")

    await page.close()
