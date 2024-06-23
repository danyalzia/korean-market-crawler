# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.yoonsung1.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.yoonsunginc.kr/products/product_view.php?Mode=I&page=1&hc=80&cn=1&sc=0&ProductSeqNo=3579&keyword=",
    }

    urls = list(urls)
    shuffle(urls)

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    try:
        data = await extract_data(browser, page, document, url, "", "")
    except Exception as err:
        raise AssertionError(f"{err} ({url})") from err

    print(f"{data = }")

    (
        *_,
        detailed_images_html_source,
    ) = data

    with open("html.html", "w", encoding="utf-8") as f:
        f.write(detailed_images_html_source)

    await page.close()
