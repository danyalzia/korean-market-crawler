from __future__ import annotations

import asyncio

from market_crawler.aqus.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser_headed: PlaywrightBrowser):
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

    images = (await extract_images(page, url, "", "")).unwrap()
    print(f"{images = }")

    await page.close()
