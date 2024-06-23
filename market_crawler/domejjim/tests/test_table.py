# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domejjim.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://www.domejjim.com/shop/shopdetail.html?branduid=599108&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=bWt3UA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=629764&xcode=009&mcode=001&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp6W10%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=576745&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bGd3UFg%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=615802&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp%2BW11N",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=626889&xcode=009&mcode=006&scode=&type=X&sort=manual&cur_code=009&GfDT=bmx1W11H",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=630640&xcode=013&mcode=001&scode=&type=Y&sort=manual&cur_code=013&GfDT=bml6W11E",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=594747&xcode=023&mcode=001&scode=&type=X&sort=manual&cur_code=023&GfDT=bmt7W1Q%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=606428&xcode=007&mcode=003&scode=&type=X&sort=order&cur_code=007&GfDT=b2V8WQ==",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=606436&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=a2l3Ug%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=610667&xcode=009&mcode=002&scode=&type=X&sort=manual&cur_code=009&GfDT=a2V1",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=578291&xcode=009&mcode=006&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp%2BW1w%3DTask",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=623561&xcode=023&mcode=003&scode=&type=X&sort=manual&cur_code=023&GfDT=bm15W14%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=632543&xcode=007&mcode=002&scode=&type=X&sort=order&cur_code=007&GfDT=bm51W10%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=515719&xcode=003&mcode=002&scode=&type=X&sort=manual&cur_code=003&GfDT=aGZ3UFw%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=632025&xcode=030&mcode=003&scode=&type=X&sort=manual&cur_code=030&GfDT=bml7W11F",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=618759&xcode=009&mcode=002&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp%2BW14%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=632006&xcode=033&mcode=002&scode=&type=X&sort=manual&cur_code=033&GfDT=aml3Vg%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=622220&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=b2V8WQ%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=629237&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=bGV0",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=620834&xcode=007&mcode=003&scode=&type=X&sort=order&cur_code=007&GfDT=a2t3WA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=610661&xcode=009&mcode=002&scode=&type=X&sort=manual&cur_code=009&GfDT=bWt3UFw=",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    table = (await extract_table(document, url)).unwrap()
    print(f"{table = }")

    table = (await extract_table(page, url)).unwrap()
    print(f"{table = }")

    await page.close()
