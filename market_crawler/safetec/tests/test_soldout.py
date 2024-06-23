# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.safetec.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_soldout_text,
    parse_document,
    visit_link,
)


async def test_soldout(browser: PlaywrightBrowser):
    urls = {
        "http://safetecb2b.co.kr/product/%EB%A5%B4%EA%B9%8C%ED%94%84-ls-300%EB%B0%A9%ED%95%9C%EC%95%88%EC%A0%84%ED%99%94/128/category/63/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    soldout_text = await extract_soldout_text(document)
    print(f"{soldout_text = }")

    await page.close()
