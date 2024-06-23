# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.bnkrod.app import (
    Err,
    Ok,
    PlaywrightBrowser,
    error,
    extract_options,
    extract_table,
    split_options_text,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = [
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000005",
        "http://www.bnkrod.co.kr/goods/goods_view.php?goodsNo=1000000064",
    ]

    tasks = [extract(url, browser_headed) for url in urls]
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = await extract_table(page, url)
    options = await extract_options(page)

    for option in options:
        if isinstance(table.price2, int):
            match await split_options_text(option, table.price2):
                case Ok(result):
                    print(result)
                case Err(err):
                    raise error.IncorrectData(
                        f"Could not split option text ({option}) into price2 due to an error -> {err}",
                        url=url,
                    )

    await page.close()
