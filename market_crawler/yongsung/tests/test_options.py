# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.yongsung.app import (
    PlaywrightBrowser,
    extract_options,
    extract_table,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100118",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100138",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100110",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100100",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100109",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100108",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100130",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100139",
        "http://shop.yong-sung.co.kr/new/itemdetails.aspx?spec=M100117",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = await extract_table(page)

    options = await extract_options(page)

    for soldout_flag, option in options:
        option1, option2, option3, price3_ = split_options_text(
            option, table.price3
        ).unwrap()
        if soldout_flag == "not soldout":
            print((option1, option2, option3, price3_))
        else:
            print((option1, "품절", option3, price3_))

    await page.close()
