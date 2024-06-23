# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.tentwentybag.app import (
    PlaywrightBrowser,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser: PlaywrightBrowser):
    urls = {
        "https://www.1020bag.com/goods/goods_view.php?goodsNo=7929",
        "https://www.1020bag.com/goods/goods_view.php?goodsNo=8039",
        "https://www.1020bag.com/goods/goods_view.php?goodsNo=18783",
        "https://www.1020bag.com/goods/goods_view.php?goodsNo=12125",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    document = await parse_document(await page.content(), engine="lxml")
    assert document

    (data) = await extract_data(browser, document, url, "", "")

    print(f"{data = }")

    await page.close()
