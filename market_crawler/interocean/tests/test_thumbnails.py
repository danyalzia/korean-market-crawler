# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

import aiohttp

from market_crawler.interocean.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    download_detailed_image,
    download_thumbnail_images,
    extract_table,
    parse_document,
    visit_link,
)


async def test_thumbnails(browser: PlaywrightBrowser):
    urls = {
        "http://interocean.co.kr/product/detail.html?product_no=2587&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=38&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=2006&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=44&cate_no=32&display_group=1",
        "http://interocean.co.kr/product/detail.html?product_no=54&cate_no=32&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    product_name, _ = await extract_table(page)

    session = aiohttp.ClientSession()
    thumbnail_images = await download_thumbnail_images(
        document, product_name, url, session
    )
    all_downloaded_images = await download_detailed_image(page, product_name, session)

    print(f"{thumbnail_images = }")

    assert all_downloaded_images, f"{page.url}"

    await page.close()
    await session.close()
