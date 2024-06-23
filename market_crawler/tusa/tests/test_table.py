# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.tusa.app import PlaywrightBrowser, extract_table, visit_link


async def test_table(browser: PlaywrightBrowser):
    urls = {
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=356",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=02000000&ps_goid=369",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=02000000&ps_goid=361",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=02000000&ps_goid=357",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=348",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=352",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=338",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=334",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_goid=324",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=376",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=356",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=348",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=350",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=353",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=352",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=338",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=349",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=368",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=355",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=354",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=347",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=342",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=339",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=344",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=341",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=337",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=340",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=343",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=351",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=324",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=334",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=335",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=336",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=331",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=333",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=330",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=328",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=322",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=68",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=67",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=323",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=319",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=321",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=317",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=325",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=332",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=329",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=318",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=1&ps_goid=66",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=28",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=58",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=43",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=49",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=48",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=56",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=45",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=51",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=33",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=57",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=50",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=44",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=59",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=34",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=41",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=37",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=35",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=46",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=52",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=47",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=24",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=21",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=15",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=25",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=22",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=14",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=16",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=20",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=27",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=23",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=7",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=8",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=10",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=9",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=19",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=17",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=26",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=13",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=18",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=01000000&ps_page=2&ps_goid=6"
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_mode=search&url=m_mall_detail.php&ps_search=SOLA+VIDEO+PRO+9600&x=0&y=0&ps_goid=304",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=03000000&ps_page=1&ps_goid=143",
        "http://shop122818.wepas.co.kr/mall/m_mall_detail.php?ps_ctid=04000000&ps_page=1&ps_goid=254",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    price2, manufacturing_country, brand_name, manufacturer = (
        await extract_table(page, url)
    ).unwrap()

    print(f"{price2 = }")
    print(f"{manufacturing_country = }")
    print(f"{brand_name = }")
    print(f"{manufacturer = }")

    await page.close()
