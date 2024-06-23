# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.yoonsung2.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    block_requests,
    extract_thumbnail_images,
    parse_document,
    visit_link,
)


async def test_thumbnails(browser_headed: PlaywrightBrowser):
    urls = {
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=REBA03982",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=NEBA69821",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=4525807144284",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=RESP045157",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=RESP04319",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=ROTR38898",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=ROSB38928",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=4525807207996",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=ROHE33645",
    }

    urls = list(urls)
    shuffle(urls)

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await page.route("**/*", block_requests)

    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    try:
        thumbnail_images = await extract_thumbnail_images(document, url)
    except Exception as err:
        raise AssertionError(f"{err} ({url})") from err

    print(f"{thumbnail_images = }")

    await page.close()
