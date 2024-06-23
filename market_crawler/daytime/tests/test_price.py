# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.daytime.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_price3,
    parse_document,
    visit_product_link,
)


async def test_price(browser: PlaywrightBrowser):
    urls = {
        "http://www.daytime.kr/goods/content.asp?guid=2102&cate=175&params=cate=162^sword=^swhat=^listsort=new^listtype=album^listsize=5^page=",
        "http://www.daytime.kr/goods/content.asp?guid=2101&cate=175&params=cate=162^sword=^swhat=^listsort=new^listtype=album^listsize=5^page=",
        "http://www.daytime.kr/goods/content.asp?guid=2090&cate=175&params=cate=162^sword=^swhat=^listsort=new^listtype=album^listsize=5^page=",
        "http://www.daytime.kr/goods/content.asp?guid=2098&cate=273&params=cate=168^sword=^swhat=^listsort=new^listtype=album^listsize=5^page=",
        "http://www.daytime.kr/goods/content.asp?guid=2124&cate=274&params=cate=168^sword=^swhat=^listsort=new^listtype=album^listsize=5^page=",
        "http://www.daytime.kr/goods/content.asp?guid=2119&cate=274&params=cate=168^sword=^swhat=^listsort=new^listtype=album^listsize=5^page=",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_product_link(
        page,
        url,
    )

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price3 = await extract_price3(document)

    print(price3, url)

    await page.close()
