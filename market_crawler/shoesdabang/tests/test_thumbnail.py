# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.shoesdabang.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_image,
    parse_document,
    visit_link,
)


async def test_thumbnail(browser: PlaywrightBrowser):
    urls = {
        "https://shoesdabang.com/product/%EA%B0%80%EB%B0%A9-az968/4763/category/31/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-440-6/4155/category/25/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-205-4/3436/category/25/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-1800/4250/category/29/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-6006/4253/category/29/display/1/",
        "https://shoesdabang.com/product/%ED%92%88%EB%B2%88-6006/4253/category/29/display/2/",
        "https://shoesdabang.com/product/%EA%B0%80%EB%B0%A9-8277/4761/category/31/display/1/",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="modest")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_image = await extract_thumbnail_image(document, url)
    assert thumbnail_image

    print(f"{thumbnail_image = }")

    await page.close()
