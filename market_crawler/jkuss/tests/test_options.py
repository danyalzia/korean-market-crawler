# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.jkuss.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://jkussmall.com/product/jc4mlm0551%EC%BD%94%EB%A6%AC%EC%95%84%EC%97%90%EB%94%94%EC%85%98/594/category/47/display/1/",
        "https://jkussmall.com/product/jd9wje0604-오로라카멜-제시백-포일-원피스/685/category/23/display/1/",
        "https://jkussmall.com/product/jdxcbk001g카멜레온-미러-수경블랙/689/category/27/display/1/",
        "https://jkussmall.com/product/jc3wko0481롤리네온/22/category/54/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page, url)).unwrap()

    print(f"{options = }")

    await page.close()
