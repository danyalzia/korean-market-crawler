# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.cutykids.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_options,
    parse_document,
    visit_link,
)


async def test_options(browser: PlaywrightBrowser):
    urls = {
        "http://www.cutykids.com/list.php?ai_id=2193167&ai_no=GANA-2A040-3c&ac_id=11&fa_comp_no=&comp_no=&mode=&comp_name_s=&all_search=&search_price=&sort=&ary=&format=jpg&s_date=&gigan=&comp_head=&pg=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    options = (await extract_options(document, page)).unwrap()
    print(f"{options = }")

    await page.close()
