# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.imac.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_data,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://www.imacmall.co.kr/goods/view?no=146",
        "https://www.imacmall.co.kr/goods/view?no=240",
        "https://www.imacmall.co.kr/goods/view?no=384",
        "https://www.imacmall.co.kr/goods/view?no=414",
        "https://www.imacmall.co.kr/goods/view?no=278",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    *_, price2, _, options, _ = await extract_data(page, content, document, url, "", "")

    print(f"{options = }")

    for option1 in options:
        print(split_options_text(option1, price2).unwrap())

    await page.close()
