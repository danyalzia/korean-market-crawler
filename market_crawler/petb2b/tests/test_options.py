# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.petb2b.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EC%98%A4%EA%B0%80%EB%8B%89-%EB%88%88%EC%84%B8%EC%A0%95%EC%A0%9C-200ml/3819/category/400/display/1/",
        "https://petbtob.co.kr/product/%EC%BA%90%EC%89%AC%EC%B6%A9%EC%A0%84%EC%A0%81%EB%A6%BD%EA%B8%88%EC%A0%84%ED%99%98/2674/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EC%95%BC%EC%98%B9%EC%9D%B4%EB%A7%98%EB%A7%88-%ED%9D%B0%EC%82%B4%EC%B0%B8%EC%B9%98-%EC%BA%94-160g-%EA%B5%AD%EB%82%B4%EC%82%B0-%EA%B7%B8%EB%A0%88%EC%9D%B8%ED%94%84%EB%A6%AC%EC%BA%94/4581/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%8C%80%EB%9F%89%EA%B5%AC%EB%A7%A4%ED%95%A0%EC%9D%B8%EA%B9%A8%EB%81%97%ED%95%98%EA%B0%9C-%EB%B0%B0%EB%B3%80%EB%B4%89%ED%88%AC-%EB%A6%AC%ED%95%84-45%EB%A7%A43%EB%A1%A4x15%EB%A7%A4/4564/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EC%95%BC%EC%98%B9%EC%9D%B4%EB%A7%98%EB%A7%88-%ED%9D%B0%EC%82%B4%EC%B0%B8%EC%B9%98%EC%99%80-%EC%97%B0%EC%96%B4-%EC%BA%94-160g-%EA%B5%AD%EB%82%B4%EC%82%B0-%EA%B7%B8%EB%A0%88%EC%9D%B8%ED%94%84%EB%A6%AC%EC%BA%94/4582/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EB%84%A4%EC%B8%84%EB%9F%B4%EC%98%A4-%EB%8B%AD%EA%B3%A0%EA%B8%B0-%EC%A0%B8%ED%82%A4-100g/4543/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EC%98%A4%EA%B0%80%EB%8B%88%EC%89%AC-%EB%B0%9C%EC%84%B8%EC%A0%95%EC%A0%9C-150ml/3828/category/400/display/1/",
        "https://petbtob.co.kr/product/%EC%95%8C%EB%9F%AC%ED%94%84%EB%9D%BC%EB%8F%84-%EC%95%8C%EB%8F%84%EB%84%9B-%ED%8C%A8%ED%82%A4%EC%A7%80/9795/category/400/display/1/'",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await visit_link(page, url)

    options1 = await extract_options(page)

    assert options1 != ""
    await page.close()
