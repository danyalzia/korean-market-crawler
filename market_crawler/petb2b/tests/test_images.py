# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.petb2b.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = [
        "https://petbtob.co.kr/product/%EA%B1%B4%EA%B0%95%ED%95%98%EA%B0%9C-%EA%B3%A0%EA%B5%AC%EB%A7%88%ED%81%90%EB%B8%8C-300g3%EA%B0%9C%EB%AC%B6%EC%9D%8C/6491/category/400/display/1/",
        "https://petbtob.co.kr/product/%EC%89%AC%EB%B0%94-%EC%B4%89%EC%B4%89%ED%95%9C-%EB%8B%AD%EA%B0%80%EC%8A%B4%EC%82%B4%EA%B3%BC-%EA%B7%B8%EB%A0%88%EC%9D%B4%EB%B9%84-%EC%86%8C%EC%8A%A4-85g-%EB%A7%88%EC%A6%88/3970/category/400/display/1/",
        "https://petbtob.co.kr/product/%EC%8B%9C%EC%A0%80-%EC%87%A0%EA%B3%A0%EA%B8%B0-100g%EB%A7%88%EC%A6%88/3866/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EC%95%BC%EC%98%B9%EC%9D%B4%EB%A7%98%EB%A7%88-%EA%B3%A0%EC%96%91%EC%9D%B4%EC%BA%94-3%EC%A2%85-%EB%AC%B6%EC%9D%8C-160g-x-3%EA%B0%9C-%EA%B5%AD%EB%82%B4%EC%82%B0-%EA%B7%B8%EB%A0%88%EC%9D%B8%ED%94%84%EB%A6%AC%EC%BA%94/4584/category/400/display/1/",
        "https://petbtob.co.kr/product/%ED%95%A0%EC%9D%B8%EC%B5%9C%EB%8C%8020%EC%9D%B4%EB%82%B4%EC%9A%94%EB%A7%9D-%EB%8B%A5%ED%84%B0%EB%8F%84%EB%B9%84-%ED%9E%99%EC%95%A4%EC%A1%B0%EC%9D%B8%ED%8A%B8-%EA%B4%80%EC%A0%88-5kg/4270/category/400/display/1/",
        "https://petbtob.co.kr/product/%ED%98%B8%EC%8B%9C%ED%83%90%ED%83%90-%EB%AA%A8%EC%9D%B4%EC%8A%A4%ED%8A%B8-%EC%98%A4%EB%A6%AC%EB%B2%84%EA%B1%B0-100g/4552/category/400/display/1/",
        "https://petbtob.co.kr/product/%EA%B5%BF%ED%94%84%EB%9E%9C%EB%93%9C-%EB%82%B4%EC%B8%84%EB%9F%B4%EB%A8%BC%EC%B9%98%EB%A1%A4-10p/4572/category/400/display/1/",
        "https://petbtob.co.kr/product/%EA%B0%93%EA%B5%AC%EC%9A%B4-%EC%B9%98%ED%82%A8%EC%B9%A9%EC%BF%A0%ED%82%A4-350g/4625/category/400/display/1/",
        "https://petbtob.co.kr/product/%EB%B0%95%EC%8A%A4%ED%95%A0%EC%9D%B8%EC%98%A4%EA%B0%80%EB%8B%88%EC%89%AC-%EB%B0%9C%EC%84%B8%EC%A0%95%EC%A0%9C-150ml/3828/category/400/display/1/",
    ]
    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).expect(url)
    print(f"{images = }")

    await page.close()
