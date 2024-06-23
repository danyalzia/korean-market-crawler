# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.shoesdabang.app import PlaywrightBrowser, extract_table, visit_link


async def test_extract_table(browser: PlaywrightBrowser):
    urls = {
        "https://shoesdabang.com/product/%EA%B0%80%EB%B0%A9-az968/4763/category/31/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-440-6/4155/category/25/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-205-4/3436/category/25/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-1800/4250/category/29/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-6006/4253/category/29/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-6006/4253/category/29/display/2/",
        "https://shoesdabang.com/product/%EA%B0%80%EB%B0%A9-8277/4761/category/31/display/1/",
        "https://www.shoesdabang.com/product/%ED%92%88%EB%B2%88-1067%ED%99%94%EC%9D%B4%ED%8A%B8-%EC%B6%94%EA%B0%80/3254/category/24/display/1/",
        "https://www.shoesdabang.com/product/%ED%92%88%EB%B2%88-5048/4138/category/24/display/1/",
        "https://www.shoesdabang.com/product/%EA%B0%80%EC%A3%BD-%EC%BF%A0%EC%85%98-%ED%8C%A8%EB%93%9C/2534/category/25/display/1/",
        "https://www.shoesdabang.com/product/1269%EB%8F%84%ED%8A%B8%EB%AF%BC%EC%9E%90-%EC%9E%98%EB%82%98%EA%B0%80%EC%9A%94/3324/category/29/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    product_name, model_name, price2 = await extract_table(page)

    print(f"{product_name = }")
    print(f"{model_name = }")
    print(f"{price2 = }")

    await page.close()
