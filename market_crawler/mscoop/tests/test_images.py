# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.mscoop.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_login: PlaywrightBrowser):
    urls = {
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31735000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31631000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31583000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3346000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3292000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3143000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3388000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=31387000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3174000",
        "http://mscoop.co.kr/site/estore/mscoop1/index.php?CID=goods_detail&CODE=3214000",
    }

    tasks = (extract(url, browser_login) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    html_source = await extract_images(page, "", "")

    print(f"{html_source = }")
    assert html_source

    await page.close()
