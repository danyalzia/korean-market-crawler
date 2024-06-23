# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.domeplay.app import PlaywrightBrowser, extract_table, visit_link


async def test_product_detail_images(browser: PlaywrightBrowser):
    urls = {
        "https://domeplay.co.kr/product/detail.html?product_no=4532&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4410&cate_no=300&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4550&cate_no=302&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3630&cate_no=297&display_group=1"
        "https://domeplay.co.kr/product/detail.html?product_no=3582&cate_no=315&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3775&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3820&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3780&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3774&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3973&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3968&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3977&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3976&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3483&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3966&cate_no=273&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3802&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=2764&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4484&cate_no=400&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3536&cate_no=308&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3515&cate_no=308&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3510&cate_no=308&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4307&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4006&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=4197&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3797&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=2747&cate_no=365&display_group=1",
        "https://domeplay.co.kr/product/detail.html?product_no=3272&cate_no=339&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)

    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    price, model_name, delivery_fee = await extract_table(page)

    assert price
    assert model_name
    assert delivery_fee

    print(f"{price = }")
    print(f"{model_name = }")
    print(f"{delivery_fee = }")

    await page.close()
