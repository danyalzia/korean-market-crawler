# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.bnkrod.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_images,
    parse_document,
)


async def test_images(browser: PlaywrightBrowser):
    urls = [
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000128",
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000004",
    ]

    tasks = [extract(url, browser) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    html_source = (await extract_images(document, url, "", "")).unwrap()
    print(f"{html_source = }")

    await page.close()
