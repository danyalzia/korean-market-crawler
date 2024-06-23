from __future__ import annotations

import asyncio

from market_crawler.domaemart.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://domaemart.com/product/%EC%95%84%EC%9D%B4%EB%8D%94-%EC%84%B8%EC%9D%B4%ED%94%84%ED%8B%B0-smart-403-white-4%EC%9D%B8%EC%B9%98/3710/category/360/display/1/",
    }

    tasks = (asyncio.create_task(extract(url, browser)) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    images = (await extract_images(page, url, "", "")).unwrap()
    print(f"{images = }")

    await page.close()
