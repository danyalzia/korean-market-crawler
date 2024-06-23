# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.helpers import chunks
from market_crawler.mscoop.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name_model_name_and_soldout,
    parse_document,
    visit_link,
)


async def test_title(
    browser_nologin: PlaywrightBrowser,
):
    urls = [
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=62461000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3174000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3292000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=61447000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=6948000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=62709000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31735000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31631000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31583000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3346000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3143000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3388000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31387000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3357000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31471000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=63225000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=32303000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31023000",
    ]
    url_list_chunks = list(chunks(urls, 3))

    for url_chunk in url_list_chunks:
        tasks = (extract(url, browser_nologin) for url in url_chunk)
        await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        product_name,
        model_name,
        sold_out_status_text,
    ) = await extract_product_name_model_name_and_soldout(document, url)

    print(f"{product_name = }")
    print(f"{model_name = }")
    print(f"{sold_out_status_text = }")

    await page.close()
