# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.nsrod.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    has_products,
    page_url,
    parse_document,
    visit_link,
)


async def test_links(browser: PlaywrightBrowser):
    urls = {
        "http://www.nsrod.co.kr/goods/catalog?code=0001",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    pageno = 1

    while True:
        category_page_url = page_url(current_url=url, next_page_no=pageno)

        await visit_link(page, category_page_url, wait_until="networkidle")

        if not (
            document := await parse_document(await page.content(), engine="lexbor")
        ):
            raise HTMLParsingError("Document is not parsed correctly", url=url)

        if not (number_of_products := await has_products(document)):
            break

        print(f"{number_of_products} (Page # {pageno})")
        pageno += 1

    await page.close()
