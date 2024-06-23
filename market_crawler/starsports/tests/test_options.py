# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.starsports.app import (
    PlaywrightBrowser,
    extract_options,
    split_options_text,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "https://starsportsmall.co.kr/goods/content.asp?guid=58368&cate=781&params=cate=781^sword=^swhat=^listsort=new^listtype=album^listsize=5^sprice=^page=",
        "https://starsportsmall.co.kr/goods/content.asp?guid=55537&cate=781&params=cate=781^sword=^swhat=^listsort=new^listtype=album^listsize=5^sprice=^page=",
        "https://starsportsmall.co.kr/goods/content.asp?guid=46153&cate=788&params=cate=788^sword=^swhat=^listsort=new^listtype=list^listsize=20^sprice=^page=1",
    }

    urls = list(urls)
    shuffle(urls)
    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    options = (await extract_options(page)).unwrap()

    print(f"{options = }")
    for option in options:
        print(split_options_text(option).unwrap())

    await page.close()
