# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sfc.app import Err, Ok, PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000090",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000099",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000071",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000021",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000096",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    match await extract_table(page):
        case Ok(table_list):
            if (
                "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000071"
                in url
            ):
                assert len(table_list) == 18

            for table in table_list:
                print(f"{table.model_name = }")
                print(f"{table.price2 = }")

            await page.close()
        case Err(err):
            print(err)
        case x:
            print(x)
