# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.blackrhino.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser_headed: PlaywrightBrowser):
    urls = {
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
    }

    tasks = (extract(browser_headed, url) for url in urls)
    await asyncio.gather(*tasks)


async def extract(browser: PlaywrightBrowser, url: str):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    table = await extract_table(page)
    print(f"{table = }")

    await page.close()
