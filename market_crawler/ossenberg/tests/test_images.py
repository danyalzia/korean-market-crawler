# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.ossenberg.app import PlaywrightBrowser, extract_images, visit_link


async def test_images(browser: PlaywrightBrowser):
    urls = {
        "http://ossenberg.co.kr/product/detail.html?product_no=36&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=28&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=45&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=119&cate_no=257&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=32&cate_no=204&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=69&cate_no=204&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=152&cate_no=50&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=29&cate_no=214&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=30&cate_no=214&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=35&cate_no=214&display_group=1",
        "http://ossenberg.co.kr/product/detail.html?product_no=53&cate_no=204&display_group=1",
    }

    tasks = (extract(url, browser) for url in urls)
    await asyncio.gather(*tasks)


async def extract(url: str, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, url, wait_until="networkidle")

    html = await extract_images(page, "PRDOUCT NAME", "", "")

    print(f"{html = }")

    await page.close()
