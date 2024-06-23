# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import sys

from contextlib import suppress
from typing import Any

import lxml.html as lxml
import pandas as pd
import playwright.async_api as playwright

from aiofile import AIOFile

from dunia.browser import BrowserConfig
from dunia.login import LoginInfo
from dunia.playwright import AsyncPlaywrightBrowser
from market_crawler import error
from market_crawler.daiwa import config
from market_crawler.daiwa.app import (
    compile_regex,
    extract_images,
    extract_thumbnail_images,
    google_translate_element,
    log,
    visit_link,
)
from market_crawler.settings import Settings
from robustify.result import Err, Ok


sys.path.insert(0, "..")
sys.path.insert(0, "../../")


async def main():
    df = pd.read_excel("DAIWA URL.xlsx")
    urls: list[str] = df["URL"].to_list()  # type: ignore
    model_names2: list[str] = df["model name 2"].to_list()  # type: ignore

    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#username",
        password_query="#password",
        login_button_query="form button:has-text('로그인')",
    )
    settings = Settings("", False, False, False, [], {}, "", "", [], "", "")
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    async with playwright.async_playwright() as async_playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=async_playwright,
            login_info=login_info,
        ).create()

        page = await browser.new_page()

        for product_url, model_name2 in zip(urls, model_names2):
            log.info(f"Visiting: <blue>{product_url}</>")
            await visit_link(page, product_url)

            match await extract_thumbnail_images(page, product_url):
                case Ok(thumbnail_images):
                    thumbnail_images = thumbnail_images[:5]
                case Err(err):
                    return None

            match await extract_images(
                page, thumbnail_images, product_url, html_top, html_bottom
            ):
                case Ok(detailed_images_html_source):
                    pass
                case Err(err):
                    raise error.ProductDetailImageNotFound(err, product_url)

            save_file = os.path.abspath(f"{model_name2}.html")

            with open(save_file, "w", encoding="utf-8") as f:
                f.write(detailed_images_html_source)

            await translate(page, detailed_images_html_source, save_file)  # type: ignore


async def translate(page: playwright.Page, html_source: str, save_file: str):
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
        async with page.expect_request_finished(timeout=2500):
            await page.select_option(query, "ko")

    # ? We will scroll into every node possible to make sure google translate snippet is able to read all the content
    for text in await page.query_selector_all("font, span, li, tr, th, div, table"):
        with suppress(error.PlaywrightTimeoutError):
            await text.scroll_into_view_if_needed(timeout=2500)

    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_selector(r"#\:1\.container", timeout=2500)

    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_load_state("domcontentloaded", timeout=2500)

    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_selector("body", state="visible", timeout=2500)

    content = await page.content()

    div: Any = lxml.fromstring(content).cssselect("div[id='COMPANY']")[0]

    match lxml.tostring(div, pretty_print=True):
        case bytes(content):
            content = content.decode("utf-8")
        case _:
            pass

    regex = compile_regex(r"cellspacing=\"(\w+)\"")
    for cellspacing in regex.findall(content):
        content = content.replace(
            f'<div class="c36022852"><table cellspacing="{cellspacing}"',
            """<div class="c36022852"><table cellspacing="10" cellpadding="10" width="100%" border="1" frame="hsides" rules="all\"""",
        )

        content = content.replace(
            f'<div class="c36022852"><table border="0" cellspacing="{cellspacing}"',
            """<div class="c36022852"><table cellspacing="10" cellpadding="10" width="100%" border="1" frame="hsides" rules="all\"""",
        )

    async with AIOFile(save_file, "w", encoding="utf-8-sig") as f:
        await f.write(content)


asyncio.run(main())
