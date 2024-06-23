# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.dangolmart.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_table,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_links(browser: PlaywrightBrowser):
    urls = {
        "https://dangolmart.shop/product/%EC%9D%B4%EC%A0%9C%EC%9D%B4%ED%91%B8%EB%93%9C-%EA%B9%80%EC%B2%9C-%EC%83%A4%EC%9D%B8%EB%A8%B8%EC%8A%A4%EC%BC%93-1kg-15kg-2kg-2kg-%EC%84%A0%EB%AC%BC%EC%9A%A9-4kg-%EC%84%A0%EB%AC%BC%EC%9A%A9-%EB%A9%B4%EC%84%B8%EC%8B%A4%EC%86%8D%ED%98%95-500g-%EA%B3%B5%EA%B8%89%EC%A4%91%EB%8B%A8/903/category/25/display/1/",
        "https://dangolmart.shop/product/detail.html?product_no=223&cate_no=23&display_group=1",
        "https://dangolmart.shop/product/detail.html?product_no=828&cate_no=53&display_group=1",
        "https://dangolmart.shop/product/detail.html?product_no=829&cate_no=53&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price2, *_ = (await extract_table(document, url)).unwrap()

    options = (await extract_options(page)).unwrap()

    for option in options:
        print(split_options_text(option, price2).unwrap())

    await page.close()
