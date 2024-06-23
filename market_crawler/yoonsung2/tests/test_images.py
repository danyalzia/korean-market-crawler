# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.yoonsung2.app import (
    PlaywrightBrowser,
    block_requests,
    extract_images,
    visit_link,
)


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=NEBA69821",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=4525807144284",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=RESP045157",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=RESP04319",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=ROTR38898",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=ROSB38928",
        "http://shop.yoonsunginc.com/new/itemdetails.aspx?spec=4525807207996",
    }

    urls = list(urls)
    shuffle(urls)

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await page.route("**/*", block_requests)

    await visit_link(page, url)

    try:
        detailed_images_html_source = (await extract_images(page, url, "", "")).unwrap()
    except Exception as err:
        print(f"{err} ({url})")
        detailed_images_html_source = ""

    print(f"{detailed_images_html_source = }")

    with open("html.html", "w", encoding="utf-8") as f:
        f.write(detailed_images_html_source)

    await page.close()
