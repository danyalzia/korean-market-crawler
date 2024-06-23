# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domejjim.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://www.domejjim.com/shop/shopdetail.html?branduid=599108&xcode=007&mcode=004&scode=&type=X&sort=order&cur_code=007&GfDT=bWt3UA%3D%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=629764&xcode=009&mcode=001&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp6W10%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=576745&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bGd3UFg%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=615802&xcode=009&mcode=005&scode=&type=X&sort=manual&cur_code=009&GfDT=bmp%2BW11N",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=626889&xcode=009&mcode=006&scode=&type=X&sort=manual&cur_code=009&GfDT=bmx1W11H",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=630640&xcode=013&mcode=001&scode=&type=Y&sort=manual&cur_code=013&GfDT=bml6W11E",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=594747&xcode=023&mcode=001&scode=&type=X&sort=manual&cur_code=023&GfDT=bmt7W1Q%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=632025&xcode=030&mcode=003&scode=&type=X&sort=manual&cur_code=030&GfDT=bmp0W11F",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=632710&xcode=007&mcode=002&scode=&type=X&sort=order&cur_code=007&GfDT=aW53Vg==",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=605166&xcode=007&mcode=002&scode=&type=X&sort=order&cur_code=007&GfDT=bm96W10=",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=569024&xcode=001&mcode=002&scode=&type=X&sort=manual&cur_code=001&GfDT=bml0W1o%3D",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=632554&search=GPA+BM790+%B3%B2%C0%DA%C7%C7%C4%A1%B9%D0%C5%B0%B1%E4%C6%C8%BA%A3%C0%CC%C1%F7%C6%BC%BC%C5%C3%F7&sort=regdate&xcode=007&mcode=004&scode=&GfDT=Z2Z3UQ==",
        "http://www.domejjim.com/shop/shopdetail.html?branduid=631441&xcode=009&mcode=001&scode=&type=X&sort=manual&cur_code=009&GfDT=bml1W11F",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options_list = (await extract_options(page)).unwrap()
    await page.close()

    print(f"{options_list = }")

    for option1 in options_list:
        option1, option2, option3, price3 = split_options_text(option1, 0)
        print(f"{option1 = }")
        print(f"{option2 = }")
        print(f"{option3 = }")
        print(f"{price3 = }")
