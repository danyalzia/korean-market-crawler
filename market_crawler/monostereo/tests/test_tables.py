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
    extract_table,
    parse_document,
)


async def test_tables(browser: PlaywrightBrowser):
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

    (
        model_name2,
        period,
        single_item_code,
        manufacturer,
        percent,
        brand,
        option4,
        message1,
    ) = (await extract_table(document)).unwrap()

    print(
        f"model_name2 = {model_name2} \n period = {period} \n single_item_code = {single_item_code} \n manufacturer = {manufacturer} \n percent = {percent} \n brand = {brand} \n option4 = {option4} \n message1 = {message1}"
    )

    await page.close()
