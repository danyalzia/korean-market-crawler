from __future__ import annotations

import asyncio

from market_crawler.aqus.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_price2,
    extract_price3,
    extract_table,
    parse_document,
    visit_link,
)


async def test_table(browser_headed: PlaywrightBrowser):
    urls = {
        "http://aqusb2b.com/view.php?num=3769&tb=&count=&category=1r03r02&pg=1",
        "http://aqusb2b.com/view.php?num=4161&tb=&count=&category=2r11&pg=3",
        "http://aqusb2b.com/view.php?num=4131&tb=&count=&category=7r18r01&pg=4",
        "http://aqusb2b.com/view.php?num=3927&tb=&count=&category=2301r02&pg=1",
    }

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    table = (await extract_table(page, url)).unwrap()
    print(f"{table = }")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    price2 = (await extract_price2(document)).unwrap()
    print(f"{price2 = }")

    price3 = (await extract_price3(document)).unwrap()
    print(f"{price3 = }")

    await page.close()
