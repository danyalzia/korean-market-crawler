# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.banax.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name,
    parse_document,
)


async def test_product_name(browser: PlaywrightBrowser):
    urls = {
        "https://banaxgallery.co.kr/sub_mall/view.php?p_idx=12740&cate=0009_0142_&cate2=0009_",
        "https://banaxgallery.co.kr/sub_mall/view.php?p_idx=13706&cate=0001_0012_&cate2=0001_",
        "https://banaxgallery.co.kr/sub_mall/view.php?p_idx=13134&cate=0008_0134_&cate2=0008_",
        "https://banaxgallery.co.kr/sub_mall/view.php?p_idx=12256&cate=0008_0133_&cate2=0008_",
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
