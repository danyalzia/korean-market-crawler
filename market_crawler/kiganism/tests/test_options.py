# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.kiganism.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000771",
        "https://www.rodall.co.kr/goods/goods_view.php?goodsNo=1000000772",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    options = (await extract_options(page)).unwrap()

    price2, *_ = (await extract_table(document)).unwrap()

    for option in options:
        option1, _price2, option2, option3 = (
            split_options_text(option, price2)
        ).unwrap()

        print(
            f"option1 = {option1} , price2 = {_price2} , option2 = {option2} , option3 = {option3}"
        )

    await page.close()
