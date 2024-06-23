# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.hyperinc.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_image,
    parse_document,
    visit_link,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "https://hyperinc.kr/product/gravitor%EA%B7%B8%EB%9D%BC%EB%B9%84%ED%84%B0-%EC%9E%90%EC%BC%93%EC%8A%88%ED%8A%B8-2mm/611/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%80%B8%ED%81%AC%EB%A3%A8%EC%A6%88-nv-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/45/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%80%B8%EC%A6%88estee-%EB%89%B4-%EB%A9%94%EA%B0%80-%ED%94%8C%EB%A0%89%EC%8A%A4-5mm-%EC%97%AC%EC%84%B1%EC%9A%A9/237/category/175/display/1/",
        "https://hyperinc.kr/product/%EC%95%84%EB%A1%9C%ED%8C%A9-tx1-%ED%88%AC%ED%94%BC%EC%8A%A4-%EC%97%AC%EC%84%B1%EC%9A%A9/210/category/175/display/1/",
        "https://hyperinc.kr/product/%EB%AA%A8%EB%B9%84%EC%8A%A4-%ED%94%84%EB%A6%AC%EB%8D%A4-%EC%9E%90%EC%BC%93-%EC%9B%BB%EC%8A%88%ED%8A%B8-%EB%82%A8%EC%84%B1%EC%9A%A9/159/category/175/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_image = (await extract_thumbnail_image(document, url)).unwrap()

    print(f"{thumbnail_image = }")

    await page.close()
