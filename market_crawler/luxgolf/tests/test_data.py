# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.luxgolf.app import PlaywrightBrowser, extract_data, visit_link


async def test_data(browser_headed: PlaywrightBrowser):
    urls = {
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=3485194&xcode=055&mcode=001&scode=005&type=X&sort=regdate&cur_code=055&GfDT=aGt3UA%3D%3D",
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=3484060&xcode=059&mcode=009&scode=&type=X&sort=manual&cur_code=059&GfDT=bmt5W1g%3D",
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=3482905&xcode=044&mcode=001&scode=001&type=X&sort=manual&cur_code=044&GfDT=bm98W11A",
        "http://www.luxgolf.net/shop/shopdetail.html?branduid=3485619&xcode=056&mcode=005&scode=&type=X&sort=manual&cur_code=056&GfDT=a253U1Q%3D",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    data = await extract_data(browser, url, "", "")
    print(f"{data = }")

    await page.close()
