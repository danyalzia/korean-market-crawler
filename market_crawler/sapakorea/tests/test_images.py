# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.sapakorea.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "http://sapakorea.co.kr/product/detail.html?product_no=1950&cate_no=93&display_group=1",
        "http://sapakorea.co.kr/product/detail.html?product_no=3577&cate_no=93&display_group=1",
        "http://sapakorea.co.kr/product/detail.html?product_no=3567&cate_no=93&display_group=1",
        "http://sapakorea.co.kr/product/detail.html?product_no=3605&cate_no=93&display_group=1",
        "http://sapakorea.co.kr/product/detail.html?product_no=3597&cate_no=93&display_group=1"
        "http://sapakorea.co.kr/product/detail.html?product_no=850&cate_no=128&display_group=1",
        "http://sapakorea.co.kr/product/detail.html?product_no=852&cate_no=128&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3491&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3454&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3453&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3381&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3380&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3379&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3378&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3377&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3376&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3308&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3304&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=3303&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1963&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1962&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1961&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1960&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1959&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1957&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1956&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1955&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1954&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1953&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1951&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=1950&cate_no=93&display_group=1",
        "https://sapakorea.co.kr/product/detail.html?product_no=7093&cate_no=129&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url)

    html_source = await extract_images(page, url, "", "")

    assert html_source
    print(f"{html_source = }")

    await page.close()
