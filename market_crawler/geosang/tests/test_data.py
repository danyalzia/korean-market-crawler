# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.geosang.app import (
    PlaywrightBrowser,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser: PlaywrightBrowser):
    urls = {
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=20528&category=074007",
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=21518&category=001004",
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=25328&category=012016",
        "https://www.geosangkorea.com/shop/goods/goods_view.php?goodsno=22128&category=061013",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    document = await parse_document(await page.content(), engine="lxml")
    assert document

    (data) = await extract_data(browser, page, document, url, "", "")

    print(f"{data = }")

    await page.close()
