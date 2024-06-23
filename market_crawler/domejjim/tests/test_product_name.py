# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domejjim.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_product_name,
    parse_document,
    visit_link,
)


async def test_product_name(browser: PlaywrightBrowser):
    urls = {
        "http://www.domejjim.com/shop/shopdetail.html?branduid=624722&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=a2V8UA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=629723&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=aWh3UQ%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=599108&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=bWt3UA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=629764&xcode=009&mcode=001&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp6W10%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=576745&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bGd3UFg%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=615802&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp%2BW11N",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=626889&xcode=009&mcode=006&scode=&type=X&sort=manual&cur_code=009&GfDT=bmx1W11H",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=630640&xcode=013&mcode=001&scode=&type=Y&sort=manual&cur_code=013&GfDT=bml6W11E",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=594747&xcode=023&mcode=001&scode=&type=X&sort=manual&cur_code=023&GfDT=bmt7W1Q%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=590895&xcode=009&mcode=004&scode=&type=X&sort=manual&cur_code=009&GfDT=bm53VQ%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=473235&xcode=003&mcode=002&scode=&type=X&sort=manual&cur_code=003&GfDT=bmp9W1w%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=568495&xcode=003&mcode=001&scode=&type=X&sort=manual&cur_code=003&GfDT=bm59W1U%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=613932&xcode=023&mcode=001&scode=&type=X&sort=manual&cur_code=023&GfDT=Zmp3VA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=579459&xcode=023&mcode=004&scode=&type=X&sort=manual&cur_code=023&GfDT=bmh3UQ%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=623561&xcode=023&mcode=003&scode=&type=X&sort=manual&cur_code=023&GfDT=bm15W14%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=577008&xcode=019&mcode=005&scode=&type=X&sort=manual&cur_code=019&GfDT=a253UFw%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=622220&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=b2V8WQ%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=536746&xcode=009&mcode=004&scode=&type=X&sort=manual&cur_code=009&GfDT=a2p3UF0%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=586945&xcode=009&mcode=004&scode=&type=X&sort=manual&cur_code=009&GfDT=bmt5W11B",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_name = await extract_product_name(document)

    print(f"{product_name = }")
