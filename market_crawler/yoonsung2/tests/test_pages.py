# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.yoonsung2.app import PlaywrightBrowser, visit_link


async def test_pages(browser_headed: PlaywrightBrowser):
    urls = {
        "http://shop.yoonsunginc.com/new/shoppinglist.aspx?itemcode=&brandcode=6100$$$$$$&search=&check=0",
    }

    urls = list(urls)
    shuffle(urls)

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(category_page_url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, category_page_url, wait_until="networkidle")

    while True:
        if not (
            current_page_no_choice := await page.text_content(
                "#innerPage div > div > div > ul > li.choice"
            )
        ):
            raise ValueError("Current page no choice not found")

        print(f"{current_page_no_choice = }")
        print(f"{category_page_url = }")

        next_page_link = await page.query_selector_all("div.paginate > ul > li")
        async with page.expect_navigation(timeout=30000):
            await next_page_link[-1].click()

        if not (
            current_page_no_choice_now := await page.text_content(
                "#innerPage div > div > div > ul > li.choice"
            )
        ):
            raise ValueError("Current page no choice not found")

        # ? If the selected page is same even after clicking on next page, it means it's the last page
        if current_page_no_choice_now == current_page_no_choice:
            break

        current_page_no_choice = current_page_no_choice_now

        category_page_url = page.url

    await page.close()
