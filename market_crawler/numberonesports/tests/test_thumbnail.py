# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

from market_crawler.numberonesports.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_image,
    gather,
    parse_document,
    visit_link,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "http://www.1sports.kr/shop/goods/goods_view.php?goodsno=48537&category=048",
        "http://www.1sports.kr/shop/goods/goods_view.php?goodsno=48309&category=048",
        "http://www.1sports.kr/shop/goods/goods_view.php?goodsno=48302&category=048",
        "http://www.1sports.kr/shop/goods/goods_view.php?goodsno=46346&category=048",
        "http://www.1sports.kr/shop/goods/goods_view.php?goodsno=46342&category=048",
        "http://www.1sports.kr/shop/goods/goods_view.php?goodsno=10816&category=056",
    }

    tasks = (extract(url, browser) for url in urls)
    await gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_image = (await extract_thumbnail_image(document, url)).unwrap()
    print(f"{thumbnail_image = }")

    await page.close()
