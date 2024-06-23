# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.deviyoga.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000236",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000238",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000237",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000235",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000174",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000173",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000175",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000169",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000172",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000168",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000171",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000170",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000167",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000158",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000156",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000154",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000153",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000155",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000157",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000150",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000148",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000152",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000151",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000149",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000275",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000276",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000066",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000065",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000047",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000046",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000194",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000193",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000192",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000191",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000123",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000120",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000122",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000121",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000118",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000119",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000116",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000113",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000112",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000111",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000117",
        "http://deviyoga.kr/goods/goods_view.php?goodsNo=1000000110",
    }

    urls = list(urls)
    shuffle(urls)
    tasks = (extract(url, browser_headed) for url in urls[:10])
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    try:
        options = (await extract_options(page, url)).unwrap()
    except Exception:
        print(f"Error URL: {url}")
        raise
    for option, price3 in options:
        print((option, price3))
        print(split_options_text(option).unwrap())

    await page.close()
