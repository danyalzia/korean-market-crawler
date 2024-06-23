# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.vinyltap.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    block_requests,
    extract_data,
    parse_document,
    visit_link,
)


async def test_data(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.vinyltap.co.uk/what-can-i-do-0666670",
        "https://www.vinyltap.co.uk/ku5055373535180-funky-disco-music",
        "https://www.vinyltap.co.uk/ca0349223001716-return-to-the-37th-chamber",
        "https://www.vinyltap.co.uk/pi5400863082802-epacr",
        "https://www.vinyltap.co.uk/antibalas-10th-anniversary-edition-0770193",
        "https://www.vinyltap.co.uk/here-without-you-0142123",
        "https://www.vinyltap.co.uk/jellies-060931",
        "https://www.vinyltap.co.uk/expensive-shit-he-miss-road-0772256",
        "https://www.vinyltap.co.uk/la0194399867716-dirt",
        "https://www.vinyltap.co.uk/wb0850007715816-live-at-maida-vale-bbc-vol-ii",
    }

    urls = list(urls)
    shuffle(urls)

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()

    await page.route("**/*", block_requests)
    await visit_link(page, url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    try:
        data = await extract_data(document, url)
    except Exception as err:
        raise AssertionError(f"{err} ({url})") from err

    print(f"{data = }")

    await page.close()
