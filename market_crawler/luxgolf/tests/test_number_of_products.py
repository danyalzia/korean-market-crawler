# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from market_crawler.luxgolf import config
from market_crawler.luxgolf.app import (
    Category,
    HTMLParsingError,
    PlaywrightBrowser,
    get_categories,
    get_products,
    parse_document,
    visit_link,
)


async def test_number_of_products(browser_headed: PlaywrightBrowser):
    categories = await get_categories(sitename=config.SITENAME)

    tasks = (extract(url, browser_headed) for url in categories[:3])

    await asyncio.gather(*tasks)


async def extract(category: Category, browser: PlaywrightBrowser):
    page = await browser.new_page()
    await visit_link(page, category.url, wait_until="domcontentloaded")

    products = (await get_products(page)).expect(category.url)
    products1_len = len(products)
    assert products1_len
    print(f"{products1_len = }")

    await visit_link(page, category.url, wait_until="load")

    products = (await get_products(page)).expect(category.url)
    products2_len = len(products)

    assert (
        products1_len == products2_len
    ), "Products loaded with 'domecontentloaded' and 'load' are not equal"

    print(f"{products2_len = }")

    content = await page.content()
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=category.url)

    products = (await get_products(document)).unwrap()
    products3_len = len(products)

    assert products3_len == products1_len == products2_len

    print(f"{products3_len = }")

    if not (document := await parse_document(content, engine="modest")):
        raise HTMLParsingError("Document is not parsed correctly", url=category.url)

    products = (await get_products(document)).unwrap()
    products4_len = len(products)

    assert products4_len == products3_len == products2_len == products1_len
    print(f"{products4_len = }")

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=category.url)

    products = (await get_products(document)).unwrap()
    products5_len = len(products)

    assert (
        products5_len
        == products4_len
        == products3_len
        == products2_len
        == products1_len
    )
    print(f"{products5_len = }")

    await page.close()
