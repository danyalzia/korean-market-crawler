# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.artinus.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_prices,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://partner.artinus.net/partner/?inc=detail&pcode=AR-964_NAVY",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AR-964_RED",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AR-966_RED",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AV-150",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AT-599R",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AT-597W",
        "http://partner.artinus.net/partner/?inc=detail&pcode=AB-561B",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    _, price2 = await extract_prices(document)
    option1 = (await extract_options(document)).unwrap()

    for option1_text, option1_value in option1.items():
        print(split_options_text(option1_text, option1_value, price2))

    await page.close()
