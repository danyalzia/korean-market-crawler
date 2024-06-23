# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.tusa.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name,
    parse_document,
    visit_link,
)


async def test_product_name(browser: PlaywrightBrowser):
    urls = {
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=356",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=02000000&ps_goid=369",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=02000000&ps_goid=361",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=02000000&ps_goid=357",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=28",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=348",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=352",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=338",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=334",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=324",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_name = (await extract_product_name(document)).unwrap()

    print(f"{product_name = }")

    await page.close()
