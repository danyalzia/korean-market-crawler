from __future__ import annotations

import asyncio

from market_crawler.danharoo.app import PlaywrightBrowser, extract_options, visit_link


async def test_options(browser_headed: PlaywrightBrowser):
    urls = {
        "http://danharoo.com/product/detail.html?product_no=52161&cate_no=161&display_group=1",
        "http://danharoo.com/product/detail.html?product_no=2995&cate_no=183&display_group=1",
    }

    tasks = (asyncio.create_task(extract(url, browser_headed)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    options = (await extract_options(page)).unwrap()
    print(f"{options = }")

    await page.close()
