# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sapakorea.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "https://sapakorea.co.kr/product/detail.html?product_no=2665&cate_no=182&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=4540&cate_no=180&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1977&cate_no=184&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=6017&cate_no=203&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=4638&cate_no=177&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document, url)).unwrap()
    print(f"{table = }")

    await page.close()
