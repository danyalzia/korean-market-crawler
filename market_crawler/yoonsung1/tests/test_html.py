# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from random import shuffle

from market_crawler.yoonsung1.app import PlaywrightBrowser, extract_images, visit_link


async def test_html(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.yoonsunginc.kr/products/product_view.php?Mode=I&page=1&hc=80&cn=1&sc=0&ProductSeqNo=3633&keyword=",
        "https://www.yoonsunginc.kr/products/product_view.php?Mode=I&page=1&hc=77&cn=1&sc=0&ProductSeqNo=3654&keyword=",
    }

    urls = list(urls)
    shuffle(urls)

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    try:
        html = (await extract_images(page, url, "", "")).unwrap()
    except Exception as err:
        raise AssertionError(f"{err} ({url})") from err

    print(f"{html = }")

    with open("html.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"{len(html) = }")

    assert len(html) < 32767

    await page.close()
