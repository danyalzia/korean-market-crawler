# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.campingb2b.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = [
        "http://campingb2b.com/goods/goods_view.php?goodsNo=1000000608",
        "http://campingb2b.com/goods/goods_view.php?goodsNo=1000001202",
        "http://campingb2b.com/goods/goods_view.php?goodsNo=1000001026",
    ]

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        model_name,
        manufacturer,
        origin,
        delivery_fee,
        price2,
        price3,
        text_other_than_price3,
        message1,
    ) = await extract_table(document, url)

    print(f"{model_name = }")
    print(f"{manufacturer = }")
    print(f"{origin = }")
    print(f"{delivery_fee = }")
    print(f"{price2 = }")
    print(f"{price3 = }")
    print(f"{text_other_than_price3 = }")
    print(f"{message1 = }")

    await page.close()
