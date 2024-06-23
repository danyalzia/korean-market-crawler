# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.daiwa.app import (
    PlaywrightBrowser,
    extract_images,
    extract_thumbnail_images,
    visit_link,
)


async def test_images(browser_headed: PlaywrightBrowser):
    urls = {
        # "https://www.daiwa.com/jp/fishing/item/lure/salt_le/kasago_mimiika_zukin/index.html",
        # "https://www.daiwa.com/jp/fishing/item/wear/shi_pan_wr/DI5220/index.html",
        # "https://www.daiwa.com/jp/fishing/item/wear/winter_wr/DW1220/index.html",
        # "https://www.daiwa.com/jp/fishing/item/rod/salt_rd/labrax_ags21/index.html",
        # "https://www.daiwa.com/jp/fishing/item/rod/egi_rd/emeraldas_mx_il_21/index.html",
        # # "https://www.daiwa.com/jp/fishing/item/special/product/megathis_ags/index.html" # ? This is very irregular page
        # "http://www.daiwa.com/global/ja/fishingshow/2019ss/certate/lineup.html",
        # "https://www.daiwa.com/jp/fishing/item/line/bass_li/bass_x_nylon/index.html",
        # "https://www.daiwa.com/jp/fishing/item/wear/winter_wr/DW1220/index.html",
        "https://www.daiwa.com/jp/fishing/item/bag/bag_bg/hg_shoulderbag_b/index.html"
    }

    tasks = (extract(url, browser_headed) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="load")

    thumbnail_images = (await extract_thumbnail_images(page, url)).unwrap()
    detailed_images = (
        await extract_images(page, thumbnail_images, url, "", "")
    ).unwrap()

    print(f"{detailed_images = }")

    with open("table.html", "w", encoding="utf-8-sig") as f:
        f.write(detailed_images)

    await page.close()
