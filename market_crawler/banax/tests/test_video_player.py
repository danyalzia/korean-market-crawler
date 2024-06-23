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
    extract_images,
    parse_document,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=13096&cate=0001_0013_&cate2=0001_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14133&cate=0001_0013_&cate2=0001_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=13706&cate=0001_0012_&cate2=0001_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14211&cate=0001_0011_&cate2=0001_"
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14312&cate=0001_0013_&cate2=0001_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14558&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=13729&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14234&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=12212&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=12656&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14144&cate=0002_0113_&cate2=0002_",
        # # # ?? iframe not found
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14309&cate=0001_0013_&cate2=0001_",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    html_source = (await extract_images(document, url, "", "")).unwrap()

    print(f"{html_source = }")

    await page.close()
