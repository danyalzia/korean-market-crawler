# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hyperinc.app import PlaywrightBrowser, extract_options, visit_link


async def test_options_new(browser: PlaywrightBrowser):
    urls = {
        "https://hyperinc.kr/product/gravitor%EA%B7%B8%EB%9D%BC%EB%B9%84%ED%84%B0-%EC%9E%90%EC%BC%93%EC%8A%88%ED%8A%B8-2mm/611/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%80%B8%ED%81%AC%EB%A3%A8%EC%A6%88-nv-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/45/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%80%B8%EC%A6%88estee-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/237/category/175/display/1/",
        "https://hyperinc.kr/product/%EC%95%84%EB%A1%9C%ED%8C%A9-tx1-%ED%88%AC%ED%94%BC%EC%8A%A4-%EC%97%AC%EC%84%B1%EC%9A%A9/210/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%94%84%EB%A6%AC%EB%8D%A4-%EC%9E%90%EC%BC%93-%EC%9B%BB%EC%8A%88%ED%8A%B8-%EB%82%A8%EC%84%B1%EC%9A%A9/159/category/175/display/1/",
        "https://hyperinc.kr/product/t4-single-hander-%EC%88%98%EC%83%81-%EB%93%9C%EB%9D%BC%EC%9D%B4%EC%8A%88%ED%8A%B8/606/category/177/display/1/#none",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-1mm-%EC%94%AC%EC%9E%90%EC%BC%93/541/category/175/display/1/#none",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EB%84%A4%EC%98%A8%EC%95%84%EC%9D%B4%EB%94%94idyl-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EC%84%B1%EC%9A%A9/75/category/175/display/1/#none",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EC%B9%B4%EB%B0%94%EC%98%88%EB%A1%9Cylrd-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EB%82%A8%EC%84%B1%EC%9A%A9/66/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EC%B9%B4%EB%B0%94%EC%98%88%EB%A1%9Cnv-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-3mm-%EB%82%A8%EC%84%B1%EC%9A%A9/60/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EC%89%AC%EC%A6%88%ED%94%BD%EC%8B%9Crd-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/51/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%EC%89%AC%EC%A6%88%ED%94%BD%EC%8B%9Cyl-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/49/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%B6%88%EC%8A%A4-%EB%B2%8C%EB%A0%88%EC%9B%8C%ED%84%B0%EB%B2%A0%EC%8A%A4%ED%8A%B8/225/category/27/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options1 = (await extract_options(page)).split(",")

    print(f"{options1 = }")

    await page.close()
