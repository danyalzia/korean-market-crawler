# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.luxgolf.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=3485106&xcode=055&mcode=001&scode=004&type=X&sort=regdate&cur_code=055&GfDT=aWl3UFo%3D",
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=89993&xcode=059&mcode=008&scode=&type=X&sort=manual&cur_code=059&GfDT=bmx7W14%3D",
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=111453&xcode=055&mcode=002&scode=004&type=X&sort=regdate&cur_code=055&GfDT=bm90W14%3D",
    }

    urls = list(urls)
    shuffle(urls)
    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price3 = (await extract_table(document)).expect(url)
    options = (await extract_options(page)).unwrap()

    print(f"{options = }")
    for option in options:
        print(split_options_text(option, price3).unwrap())

    await page.close()
