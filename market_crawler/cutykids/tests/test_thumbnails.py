# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.cutykids.app import (
    HTMLParsingError,
    PlaywrightBrowser,
    extract_thumbnail_images,
    parse_document,
    visit_link,
)


async def test_thumbnails(browser: PlaywrightBrowser):
    urls = {
        "http://www.cutykids.com/list.php?ai_id=2151980&ai_no=BI12-1D031-1&ac_id=11&fa_comp_no=&comp_no=&mode=&comp_name_s=&all_search=&search_price=&sort=&ary=&format=jpg&s_date=&gigan=&comp_head=&pg=24",
        "http://www.cutykids.com/list.php?ai_id=2212546&ai_no=B080-2B209-1c&ac_id=124&fa_comp_no=&comp_no=&mode=&comp_name_s=&all_search=&search_price=&sort=&ary=&format=jpg&s_date=&gigan=&comp_head=&pg=1",
        "http://www.cutykids.com/list.php?ai_id=2055731&ai_no=O006-1B100-17&ac_id=124&fa_comp_no=&comp_no=&mode=&comp_name_s=&all_search=&search_price=&sort=&ary=&format=jpg&s_date=&gigan=&comp_head=&pg=1&ckattempt=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=url)

    thumbnail_images = (await extract_thumbnail_images(document, url)).unwrap()

    print(f"{thumbnail_images = }")

    await page.close()
