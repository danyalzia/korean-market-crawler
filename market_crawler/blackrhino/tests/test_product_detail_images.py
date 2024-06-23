# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.blackrhino.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_images,
    parse_document,
    visit_link,
)


async def test_images(browser: PlaywrightBrowser):
    urls = [
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3328&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3325&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3326&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3321&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3316&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3307&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3297&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3248&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3189&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3181&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3149&category=041",
        "http://mongtang.co.kr/shop/goods/goods_view.php?goodsno=3131&category=041",
    ]

    tasks = (extract(browser, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    images = await extract_images(document, url, "", "")
    print(f"{images = }")

    await page.close()
