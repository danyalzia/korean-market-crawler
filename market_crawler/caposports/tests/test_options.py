from __future__ import annotations

import asyncio

from market_crawler.caposports.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    parse_document,
    visit_link,
)


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "http://www.caposports.co.kr/main/goods_view.html?uid=NADSGV9551",
    }

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    options1, options2 = (await extract_options(document)).unwrap()
    print(f"{options1 = }")
    print(f"{options2 = }")

    await page.close()
