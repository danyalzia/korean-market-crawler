# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domeplay.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "https://domeplay.co.kr/product/detail.html?product_no=4456&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4532&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4549&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4450&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4451&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4452&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4453&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4381&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4382&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4408&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4449&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4377&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4378&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4379&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4380&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4550&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3630&cate_no=297&display_group=1"
        "https://domeplay.co.kr/product/detail.html?product_no=3582&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3775&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3820&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3780&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3774&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3973&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3968&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3976&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3483&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3966&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4307&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3802&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=2764&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4484&cate_no=400&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3536&cate_no=308&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3515&cate_no=308&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3510&cate_no=308&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4610&cate_no=297&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3977&cate_no=273&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    detailed_images_html_source = (await extract_images(page, url, "", "")).unwrap()
    print(f"{detailed_images_html_source = }")

    await page.close()
