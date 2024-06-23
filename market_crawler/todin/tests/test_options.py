# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.todin.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://www.todingolf.com/goods/view?no=272",
        "https://www.todingolf.com/goods/view?no=266",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        price3,
        _,
        _,
    ) = (await extract_table(document)).unwrap()

    options = await extract_options(document)

    for option in options:
        print(split_options_text(option, price3))

    await page.close()
