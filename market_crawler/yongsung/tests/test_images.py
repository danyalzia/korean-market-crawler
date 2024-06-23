# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.yongsung.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://www.yong-sung.co.kr/default/product/all_product.php?com_board_basic=read_form&com_board_idx=557&&com_board_search_code=&com_board_search_value1=&com_board_search_value2=&com_board_page=&&com_board_id=8&&com_board_id=8",
        "https://www.yong-sung.co.kr/default/product/all_product.php?com_board_basic=read_form&com_board_idx=519&&com_board_search_code=&com_board_search_value1=&com_board_search_value2=&com_board_page=&&com_board_id=8&&com_board_id=8",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = await extract_images(page, "", "")

    print(f"{images = }")
    assert images

    await page.close()
