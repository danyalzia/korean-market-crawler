# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.playwright import PlaywrightBrowser
from market_crawler.bagissue.app import (
    HTMLParsingError,
    ensure_login,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.bagissue.co.kr/product/%EC%BA%A3%ED%82%B7-a815/1694/category/70/display/1/",
        "https://www.bagissue.co.kr/product/%ED%94%BC%EB%8B%88%EC%8A%A4-a788/1657/category/71/display/1/",
        "https://www.bagissue.co.kr/product/%EB%B2%84%ED%8B%B0%EC%BB%AC-a842/1701/category/71/display/1/",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    await ensure_login(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    try:
        table = (await extract_table(document, url)).unwrap()
    except Exception:
        await page.pause()
        raise

    print(f"{table = }")

    await page.close()
