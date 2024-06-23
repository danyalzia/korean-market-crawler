# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.daiwa.app import (
    PlaywrightBrowser,
    extract_thumbnail_images,
    visit_link,
)


async def test_thumbnail(browser_headed: PlaywrightBrowser):
    urls = {
        "https://www.daiwa.com/jp/fishing/item/lure/salt_le/kasago_mimiika_zukin/index.html",
        "https://www.daiwa.com/jp/fishing/item/wear/shi_pan_wr/DI5220/index.html",
        "https://www.daiwa.com/jp/fishing/item/wear/winter_wr/DW1220/index.html",
        "https://www.daiwa.com/jp/fishing/item/rod/salt_rd/labrax_ags21/index.html",
        "https://www.daiwa.com/jp/fishing/item/rod/egi_rd/emeraldas_mx_il_21/index.html",
        # "https://www.daiwa.com/jp/fishing/item/special/product/megathis_ags/index.html" # ? This is very irregular page
        "http://www.daiwa.com/global/ja/fishingshow/2019ss/certate/lineup.html",
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    thumbnail_images = (await extract_thumbnail_images(page, url)).unwrap()

    print(f"{thumbnail_images = }")

    await page.close()
