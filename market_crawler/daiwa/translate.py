# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import sys

from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from datetime import datetime
from glob import glob
from multiprocessing import cpu_count, freeze_support
from typing import Any, cast

import lxml.html as lxml
import pandas as pd
import playwright.async_api as playwright

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser
from market_crawler import error, log
from market_crawler.daiwa import config
from market_crawler.daiwa.app import (
    BrowserConfig,
    chunks,
    compile_regex,
    get_productid,
    google_translate_element,
)
from market_crawler.excel import get_column_mapping


sys.path.insert(0, "..")
sys.path.insert(0, "../..")


async def _translate(page: playwright.Page, html_source: str, save_file: str):
    await page.set_content(html_source)
    content = await page.content()

    body_regex = compile_regex(r"""(<body>)""")
    content = body_regex.sub(f"""<body>{google_translate_element()}""", content)

    async with AIOFile(save_file, "w", encoding="utf-8-sig") as f:
        await f.write(content)

    await page.goto(
        f"file:///{save_file}",
        wait_until="load",
    )

    if os.path.exists(save_file):
        os.remove(save_file)

    query = 'select[class="goog-te-combo"]'
    with suppress(error.PlaywrightTimeoutError):
        async with page.expect_request_finished():
            await page.select_option(query, "ko")

    # ? We will scroll into every node possible to make sure google translate snippet is able to read all the content
    for text in await page.query_selector_all("font, span, li, tr, th, div"):
        with suppress(error.PlaywrightTimeoutError):
            await text.scroll_into_view_if_needed()

    await page.wait_for_selector(r"#\:1\.container")
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_selector("body", state="visible")

    content = await page.inner_html("body")

    div: Any = lxml.fromstring(content).cssselect("div[id='COMPANY']")[0]  # type: ignore

    match lxml.tostring(div, pretty_print=True):
        case bytes(content):
            content = content.decode("utf-8")
        case _:  # type: ignore
            pass

    async with AIOFile(save_file, "w", encoding="utf-8-sig") as f:
        await f.write(content)


async def main():
    browser_config = BrowserConfig(
        headless=False,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )

    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright
        ).create()

        date: str = datetime.now().strftime("%Y%m%d")
        temp_folder = os.path.join(os.path.dirname(__file__), "temp", date)
        save_dir = os.path.join(temp_folder, "constructed")
        os.makedirs(save_dir, exist_ok=True)

        column_mapping = get_column_mapping("column_mapping.json")
        df: pd.Series[Any] = pd.concat(await concat_df_from_dir(temp_folder))  # type: ignore
        df = df.drop_duplicates(subset=[column_mapping.PRODUCT_URL_COLUMN])  # type: ignore
        urls: list[str] = list(df[column_mapping.PRODUCT_URL_COLUMN])  # type: ignore
        images: list[str] = list(df[column_mapping.DETAILED_IMAGES_HTML_SOURCE_COLUMN])  # type: ignore

        log.warning(f"Total images: {len(images)}")
        for chunk in chunks(range(len(images)), 25):
            tasks = (
                translate_detailed_images(browser, save_dir, urls[idx], images[idx])
                for idx in chunk
            )
            await asyncio.gather(*tasks)


async def translate_detailed_images(
    browser: PlaywrightBrowser, save_dir: str, product_url: str, html_source: str
):
    # ? We will use context directory for methods flexibility
    page = await browser.new_page()
    productid = get_productid(product_url).expect(
        f"Product ID not found: {product_url}"
    )
    save_file = os.path.abspath(os.path.join(save_dir, f"{productid}.html"))

    log.info(f"Translating: {productid}")

    if "not present" in html_source:
        await page.close()
        return

    await _translate(page, html_source, save_file)

    await page.close()


# ? This doesn't use pyarrow for read_csv() which gives error for some data present in DAIWA files
async def concat_df_from_dir(directory: str):
    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        results = [
            executor.submit(
                cast(Any, pd.read_csv),
                os.path.join(directory, filename),  # type: ignore
                encoding="utf-8-sig",
                dtype=str,
            )
            for filename in sorted(glob(os.path.join(directory, "*_temporary.csv")))
        ]

    return [r.result() for r in results]


if __name__ == "__main__":
    freeze_support()
    asyncio.run(main())
