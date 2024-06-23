# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domejjim.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_images,
    parse_document,
    visit_link,
)


async def test_product_detail_images(browser: PlaywrightBrowser):
    urls = {
        "http://www.domejjim.com/shop/shopdetail.html?branduid=599108&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=bWt3UA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=629764&xcode=009&mcode=001&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp6W10%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=576745&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bGd3UFg%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=615802&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp%2BW11N",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=626889&xcode=009&mcode=006&scode=&type=X&sort=manual&cur_code=009&GfDT=bmx1W11H",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=630640&xcode=013&mcode=001&scode=&type=Y&sort=manual&cur_code=013&GfDT=bml6W11E",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=594747&xcode=023&mcode=001&scode=&type=X&sort=manual&cur_code=023&GfDT=bmt7W1Q%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=635504&search=KAA%2B0202M%2B%25B3%25B2%25C0%25DA%25B1%25E2%25B4%25C9%25BC%25BA%25BD%25BA%25C6%25F7%25C3%25F7%25B9%25DD%25C6%25C8%25C6%25BC&sort=regdate&xcode=007&mcode=004&scode=&GfDT=aml3UQ%3D%3D",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    html_source = await extract_images(document, url, "", "")

    print(f"{html_source = }")
    assert html_source
