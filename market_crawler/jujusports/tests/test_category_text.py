# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.jujusports.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_category_text,
    parse_document,
    visit_link,
)


async def test_category_text(browser: PlaywrightBrowser):
    urls = {
        "https://www.jujub2b.co.kr/product/%EC%97%85%ED%8A%BC-%EC%98%A4%EB%B2%84%ED%95%8F-%ED%81%AC%EB%A3%A8%EB%84%A5-%EB%9F%BD%EB%AF%B8%EB%AF%BC%EC%9E%90-%ED%99%94%EC%9D%B4%ED%8A%B8-%EB%B0%98%ED%8C%94-%EB%9D%BC%EC%9A%B4%EB%93%9C%ED%8B%B0/9214/category/32/display/1/",
        "https://www.jujub2b.co.kr/product/%EB%B0%94%EB%B3%BC%EB%9E%8F-182416-%ED%93%A8%EC%96%B4%EB%93%9C%EB%9D%BC%EC%9D%B4%EB%B8%8C-%ED%88%AC%EC%96%B4-21-%ED%85%8C%EB%8B%88%EC%8A%A4%EB%9D%BC%EC%BC%93%EC%82%AC%EC%9D%80%ED%92%88%EC%A0%9C%EC%99%B8/3801/category/49/display/1/",
        "https://www.jujub2b.co.kr/product/%EB%B0%94%EB%B3%BC%EB%9E%8F-101357-169761-%ED%93%A8%EC%96%B4%EC%97%90%EC%96%B4%EB%A1%9C-%ED%8C%80-%ED%85%8C%EB%8B%88%EC%8A%A4%EB%9D%BC%EC%BC%93/3783/category/49/display/1/",
        "https://www.jujub2b.co.kr/product/%EB%B0%94%EB%B3%BC%EB%9E%8F-169785-%ED%93%A8%EC%96%B4%EC%97%90%EC%96%B4%EB%A1%9C-%EC%8A%88%ED%8D%BC%EB%9D%BC%EC%9D%B4%ED%8A%B8-%ED%85%8C%EB%8B%88%EC%8A%A4%EB%9D%BC%EC%BC%93/3785/category/49/display/1/",
        "https://www.jujub2b.co.kr/product/%EC%97%85%ED%8A%BC-ufo-%EC%98%A4%EB%B2%84%ED%95%8F-%ED%9B%84%EB%94%94-%EB%A7%A8%ED%88%AC%EB%A7%A8-%EB%B8%94%EB%9E%99-%EA%B8%B4%ED%8C%94-%EB%9D%BC%EC%9A%B4%EB%93%9C%ED%8B%B0/8447/category/33/display/1/",
        "https://www.jujub2b.co.kr/product/%EC%97%85%ED%8A%BC-%EC%98%A4%EC%95%84%EC%8B%9C%EC%8A%A4-%EB%9D%BC%EC%9D%B4%ED%8A%B8%EA%B8%B0%EB%AA%A8-wh-%EC%9A%B0%EB%A8%BC%EC%A6%88-%EA%B8%B4%ED%8C%94-%EB%9D%BC%EC%9A%B4%EB%93%9C%ED%8B%B0/8488/category/33/display/1/",
        "https://www.jujub2b.co.kr/product/%EC%97%85%ED%8A%BC-%ED%81%AC%EB%A6%AC%EC%8A%A4%EB%A7%88%EC%8A%A4-%EB%9D%BC%EC%9D%B4%ED%8A%B8%EA%B8%B0%EB%AA%A8-bk-%EB%A7%A8%EC%A6%88-%EA%B8%B4%ED%8C%94-%EB%9D%BC%EC%9A%B4%EB%93%9C%ED%8B%B0/8522/category/33/display/1/",
        "https://www.jujub2b.co.kr/product/%EB%B9%84%ED%8A%B8%EB%A1%9C-pzrw-12282u-%EC%9D%B4%EB%AA%A8%EC%85%94%EB%84%90-%EC%8A%A4%ED%8F%AC%EC%B8%A0-%EB%B0%94%EB%9E%8C%EB%A7%89%EC%9D%B4/8825/category/33/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    category_text = (await extract_category_text(document)).unwrap()
    print(f"{category_text = }")

    await page.close()
