from __future__ import annotations

import asyncio

from market_crawler.aqus.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    extract_price2,
    parse_document,
    split_options_text,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        # "http://aqusb2b.com/view.php?num=3769&tb=&count=&category=1r03r02&pg=1",
        # "http://aqusb2b.com/view.php?num=4161&tb=&count=&category=2r11&pg=3",
        # "http://aqusb2b.com/view.php?num=4131&tb=&count=&category=7r18r01&pg=4",
        # "http://aqusb2b.com/view.php?num=3927&tb=&count=&category=2301r02&pg=1",
        "http://aqusb2b.com/view.php?num=715&tb=&count=&category=2r13&pg=1",
    }

    tasks = (asyncio.create_task(extract(url, browser)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price2 = (await extract_price2(document)).unwrap()

    options = (await extract_options(page)).unwrap()
    print(f"{options = }")

    for option in options:
        print(split_options_text(option, price2).unwrap())

    await page.close()
