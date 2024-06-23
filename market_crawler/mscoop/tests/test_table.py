# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.mscoop.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser_login: PlaywrightBrowser):
    urls = {
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3255000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31215000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3409000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31631000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31583000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3346000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3292000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3143000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3388000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31387000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3174000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=12109000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31627000",
    }

    tasks = (extract(url, browser_login) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        price3,
        price2,
        delivery_fee,
    ) = (await extract_table(document)).unwrap()

    assert price3
    assert price2
    assert delivery_fee

    await page.close()
