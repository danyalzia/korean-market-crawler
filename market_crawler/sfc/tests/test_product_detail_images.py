# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sfc.app import PlaywrightBrowser, extract_images, visit_link


async def test_product_detail_images(browser: PlaywrightBrowser):
    urls = {
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000090",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000099",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000023",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000005",
        "http://www.xn--9t4b29bmob475q.com/goods/goods_view.php?goodsNo=1000000085",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await visit_link(page, url)

    html_source = (await extract_images(page, url, "", "")).unwrap()

    print(f"{html_source = }")
    assert html_source

    with open("html.html", "w") as f:
        f.write(html_source)

    await page.close()
