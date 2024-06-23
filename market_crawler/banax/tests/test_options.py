# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dunia.extraction import visit_link
from market_crawler.banax.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_table,
    get_option_price_and_soldout,
    parse_document,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14133&cate=0001_0013_&cate2=0001_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14558&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=13729&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14234&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=12212&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=12656&cate=0004_0022_&cate2=0004_",
        "http://www.banaxgallery.co.kr/sub_mall/view.php?p_idx=14144&cate=0002_0113_&cate2=0002_",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    manufacturing_country, maker, model_names, options_price2_soldout_list = (
        await extract_table(document)
    ).unwrap()

    print(f"{manufacturing_country = }")
    print(f"{maker = }")
    print(f"{model_names = }")

    for model_name in model_names:
        print(f"{model_name = }")

    print(f"{options_price2_soldout_list = }")

    for options_price2_soldout in options_price2_soldout_list:
        option1, price2, sold_out_text = (
            await get_option_price_and_soldout(options_price2_soldout)
        ).unwrap()

        print(f"{option1 = }")
        print(f"{price2 = }")
        print(f"{sold_out_text = }")

    await page.close()
