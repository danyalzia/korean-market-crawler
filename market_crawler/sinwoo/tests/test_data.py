# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sinwoo.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser: PlaywrightBrowser):
    urls = {
        "https://www.sinwoo.com/shop/detail.htm?brandcode=2201-0226&page=1&cat_code=48/57/&sort=default&rows=7&cat_search=&cat_code=48/57/&pick=",
        "https://www.sinwoo.com/shop/detail.htm?brandcode=2201-0208&page=1&cat_code=48/57/&sort=default&rows=7&cat_search=&cat_code=48/57/&pick=",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        price2,
        price3,
        delivery_fee,
        message1,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, url, "", "")

    print(f"{thumbnail_image_url = }")
    print(f"{thumbnail_image_url2 = }")
    print(f"{thumbnail_image_url3 = }")
    print(f"{thumbnail_image_url4 = }")
    print(f"{thumbnail_image_url5 = }")
    print(f"{product_name = }")
    print(f"{price2 = }")
    print(f"{price3 = }")
    print(f"{delivery_fee = }")
    print(f"{message1 = }")
    print(f"{options = }")
    print(f"{detailed_images_html_source = }")

    await page.close()
