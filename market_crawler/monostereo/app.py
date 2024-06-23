# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os
import re

from functools import cache
from typing import Any
from urllib.parse import urljoin

from aiofile import AIOFile
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError, LoginInputNotFound, PasswordInputNotFound
from dunia.extraction import load_content, load_page, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import PlaywrightBrowser, PlaywrightElementHandle, PlaywrightPage
from dunia.playwright.browser import AsyncPlaywrightBrowser
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.memory import MemoryOptimizer
from market_crawler.monostereo import config
from market_crawler.monostereo.data import MonostereoCrawlData
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from playwright_stealth import stealth_async
from robustify.result import Err, Ok, Result, returns_future


@cache
def extract_price_with_decimalpoint(text: str) -> str:
    # ? In MONOSTEREO, most prices contain the floating point values
    try:
        return "".join(compile_regex(r"(\d*\.\d+|\d+)").findall(text))
    except ValueError as e:
        raise ValueError(f"Text don't have any digit: '{text}'") from e


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")
    if "&p" in current_url:
        return current_url.replace(f"&p={next_page_no-1}", f"&p={next_page_no}")

    return f"{current_url}&p={next_page_no}"


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "https://b2b.monostereo1stop.com/", wait_until="networkidle")

    categories = await page.query_selector_all("#mainmenu li.level-top")

    for category in categories:
        if not (text := await category.text_content()):
            continue

        text = text.strip()

        if text not in [
            "Home",
            "Contact",
        ]:  # ? We don't need to crawl these categories
            if not (el := await category.query_selector("a")):
                continue

            if not (category_text := await el.text_content()):
                continue

            category_text = category_text.strip()

            if not (category_page_url := await el.get_attribute("href")):
                continue

            category_page_url = urljoin(page.url, category_page_url)

            # ? Visit each category to extract sub categories
            page = await browser.new_page()
            await visit_link(page, category_page_url)

            subcategories = await page.query_selector_all(
                "#narrow-by-list > div:nth-last-child(1) > div.filter-options-content.show > ol > li > a"
            )

            # ? Not all main categories may have sub categories
            if not subcategories:
                full_subcategories.append(Category(category_text, category_page_url))

            for subcategory in subcategories:
                if not (
                    subcategory_page_url := await subcategory.get_attribute("href")
                ):
                    continue

                if not (subcategory_text := await subcategory.text_content()):
                    continue

                subcategory_text = subcategory_text.strip()

                if (el := await subcategory.query_selector(".count")) and (
                    extra_text := await el.text_content()
                ):
                    subcategory_text = subcategory_text.replace(extra_text, "").strip()

                url = urljoin(
                    page.url,
                    subcategory_page_url,
                )
                full_text = f"{category_text}>{subcategory_text}"
                full_subcategories.append(Category(full_text, url))

            await page.close()

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


async def login(login_info: LoginInfo, browser: PlaywrightBrowser) -> None:
    page = await browser.new_page()
    await stealth_async(page)

    await page.goto(login_info.login_url, wait_until="networkidle")

    if await page.query_selector("a:text('Sign Out')"):
        log.success(
            f"Logged in <MAGENTA><w>(ID: {login_info.user_id}, PW: {login_info.password})</></>"
        )

        await page.close()
        return

    # ? We need to click on "Login" button to display the form for entering user id and password
    query = "a[id='customer-login']"
    if login_button := await page.wait_for_selector(query):
        await login_button.click()

    await page.wait_for_selector(
        login_info.user_id_query,
        state="visible",
    )
    await page.wait_for_selector(
        login_info.password_query,
        state="visible",
    )

    if input_id := await page.query_selector(login_info.user_id_query):
        await input_id.fill(login_info.user_id)
    else:
        raise LoginInputNotFound(f"User ID ({login_info.user_id}) could not be entered")

    if login_info.keep_logged_in_check_query:
        await page.check(login_info.keep_logged_in_check_query)

    if input_password := await page.query_selector(
        login_info.password_query,
    ):
        await input_password.fill(login_info.password)
    else:
        raise PasswordInputNotFound(
            f"Passowrd ({login_info.password}) could not be entered"
        )

    await login_info.login_button_strategy(page, login_info.login_button_query)

    log.success(
        f"Logged in <MAGENTA><w>(ID: {login_info.user_id}, PW: {login_info.password})</></>"
    )

    await page.close()


async def login_button_strategy(page: PlaywrightPage, login_button_query: str) -> None:
    async with page.expect_navigation():
        await page.click(login_button_query)

    await page.wait_for_selector("a:text('Sign Out')", state="visible")
    await page.wait_for_load_state("networkidle")


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[id='email']",
        password_query="input[id='pass']",
        login_button_query="button.login",
        login_button_strategy=login_button_strategy,
    )


async def run(settings: Settings):
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
        user_data_dir="",
    )
    login_info = get_login_info()

    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
        ).create()
        await login(login_info, browser)

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            if await asyncio.to_thread(os.path.exists, "subcategories.txt"):
                subcategories = await get_categories(
                    sitename=config.SITENAME, filename="subcategories.txt"
                )
            else:
                subcategories = await find_subcategories(browser)

            log.detail.total_categories(len(subcategories))

            crawler = ConcurrentCrawler(
                categories=subcategories,
                start_category=config.START_CATEGORY,
                end_category=config.END_CATEGORY,
                chunk_size=config.CATEGORIES_CHUNK_SIZE,
                crawl=crawl,
            )
            await crawl_categories(crawler, browser, settings, columns)
            return None

        await crawl_urls(
            list(dict.fromkeys(settings.URLS)),
            browser,
            settings,
            columns,
        )


async def crawl_urls(
    urls: list[str], browser: PlaywrightBrowser, settings: Settings, columns: list[str]
):
    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )

    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(idx, browser, urls[idx], filename, settings, columns)
            for idx in chunk
        )

        await asyncio.gather(*tasks)


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    product_url: str,
    filename: str,
    settings: Settings,
    columns: list[str],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    productid = get_productid(product_url).expect(
        f"Product ID is not found in URL ({product_url})"
    )

    if not (
        product_state := await get_product_state(
            config=config,
            productid=productid,
            category_name="CUSTOM",
            date=settings.DATE,
        )
    ):
        return None

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        thumbnail_image_url,
        category_name,
        product_name,
        model_name,
        option3,
        message3,
        quantity,
        price2,
        price3,
        option2,
        model_name2,
        period,
        message4,
        manufacturer,
        percent,
        brand,
        option4,
        message1,
        detailed_images_html_source,
        message2,
        delivery_fee,
        option1,
    ) = await extract_data(page, document, browser, product_url, html_top, html_bottom)

    await page.close()

    crawl_data = MonostereoCrawlData(
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        category=category_name,
        product_name=product_name,
        model_name=model_name,
        option3=option3,
        message3=message3,
        quantity=quantity,
        price2=price2,
        price3=price3,
        option2=option2,
        model_name2=model_name2,
        period=period,
        message4=message4,
        manufacturer=manufacturer,
        percent=percent,
        brand=brand,
        option4=option4,
        message1=message1,
        detailed_images_html_source=detailed_images_html_source,
        message2=message2,
        delivery_fee=delivery_fee,
        option1=option1,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_custom_url_crawled(idx, crawl_data.product_url)

    return None


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    category_url = category.url
    category_name = category.name

    if not (
        category_state := await get_category_state(
            config=config,
            category_name=category_name,
            date=settings.DATE,
        )
    ):
        return None

    category_html = CategoryHTML(
        name=category_name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
    )
    category_page_url = page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    # ? Replace "<" with "\<" due to this error:
    # ? ValueError: Tag "<COPINGMECHANISM>" does not correspond to any known ansi directive, make sure you did not misspelled it (or prepend '\' to escape it)
    log.action.visit_category(category_name.replace("<", r"\<"), category_page_url)

    memory_optimizer = MemoryOptimizer(
        max_products_chunk_size=config.MAX_PRODUCTS_CHUNK_SIZE,
    )
    category_chunk_size = config.CATEGORIES_CHUNK_SIZE
    products_chunk_size = config.MIN_PRODUCTS_CHUNK_SIZE

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

        page = await load_page(
            browser=browser,
            url=category_page_url,
            html=category_html,
            on_failure="visit",
            rate_limit=config.DEFAULT_RATE_LIMIT,
            async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
            wait_until="networkidle",
        )
        content = await page.content()
        if config.SAVE_HTML and not await category_html.exists():
            await category_html.save(content)

        await page.close()

        if not (document := await parse_document(content, engine="lexbor")):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )

        if not (number_of_products := await has_products(document)):
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        log.detail.total_products_on_page(number_of_products, category_state.pageno)

        filename: str = temporary_csv_file(
            sitename=config.SITENAME,
            date=settings.DATE,
            category_name=category.name,
            page_no=category_state.pageno,
        )

        products_chunk_size = await memory_optimizer.optimize_products_chunk_sizes(
            browser, category_chunk_size, products_chunk_size
        )

        for chunk in chunks(range(number_of_products), products_chunk_size):
            tasks = (
                extract_product(
                    idx,
                    browser,
                    category_page_url,
                    category_state,
                    category_html,
                    filename,
                    columns,
                    settings,
                )
                for idx in chunk
            )

            await asyncio.gather(*tasks)

        log.action.category_page_crawled(
            category_name.replace("<", r"\<"), category_state.pageno
        )

        category_state.pageno += 1
        category_html.pageno = category_state.pageno

        if config.USE_CATEGORY_SAVE_STATES:
            await category_state.save()

    category_state.done = True
    if config.USE_CATEGORY_SAVE_STATES:
        await category_state.save()


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\d")

    return (
        Ok("".join(match))
        if (match := regex.findall(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document) -> int | None:
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = ".products > li"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    category_page_url: str,
    category_state: CategoryState,
    category_html: CategoryHTML,
    filename: str,
    columns: list[str],
    settings: Settings,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    content = await load_content(
        browser=browser,
        url=category_page_url,
        html=category_html,
        on_failure="fetch",
        wait_until="networkidle",
        async_timeout=config.DEFAULT_ASYNC_TIMEOUT,
        rate_limit=config.DEFAULT_RATE_LIMIT,
    )
    if config.SAVE_HTML and not await category_html.exists():
        await category_html.save(content)

    if not (document := await parse_document(content, engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    if not (product_url := (await get_product_link(product, category_page_url)).ok()):
        page = await browser.new_page()
        await visit_link(page, category_page_url, wait_until="networkidle")

        content = await page.content()
        if config.SAVE_HTML and not await category_html.exists():
            await category_html.save(content)
        await page.close()

        if not (document := await parse_document(content, engine="lxml")):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )

        match await get_products(document):
            case Ok(products):
                product = products[idx]
            case Err(err):
                raise error.ProductsNotFound(err, url=category_page_url)

        match await get_product_link(product, category_page_url):
            case Ok(product_url):
                pass
            case Err(err):
                raise error.ProductLinkNotFound(err, url=category_page_url)

    productid = get_productid(product_url).expect(
        f"Product ID is not found in URL ({product_url})"
    )

    if not (
        product_state := await get_product_state(
            config=config,
            productid=productid,
            category_name=category_state.name,
            date=category_state.date,
        )
    ):
        return None

    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    if await is_bad_link(page):
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    for retry in range(1, 11):
        try:
            (
                thumbnail_image_url,
                category_name,
                product_name,
                model_name,
                option3,
                message3,
                quantity,
                price2,
                price3,
                option2,
                model_name2,
                period,
                message4,
                manufacturer,
                percent,
                brand,
                option4,
                message1,
                detailed_images_html_source,
                message2,
                delivery_fee,
                option1,
            ) = await extract_data(
                page, document, browser, product_url, html_top, html_bottom
            )
        except (
            error.PlaywrightError,
            error.QueryNotFound,
            error.CategoryTextNotFound,
            error.OptionsNotFound,
        ) as err:
            text = str(err)
            # ? Fix the loguru's mismatch of <> tag for ANSI color directive
            if source := compile_regex(r"\<\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
            if source := compile_regex(r"\<\/\w*\>").findall(text):
                text = text.replace(source[0], source[0].replace("</", "<"))
            if source := compile_regex(r"\<.*\>").findall(text):
                text = text.replace(source[0], source[0].replace("<", r"\<"))
                text = text.replace(source[0], source[0].replace("</", "<"))
            log.error(text)
            log.warning(f"Retrying for # {retry} times ({product_url}) ...")

            await asyncio.sleep(1)
            await page.close()
            page = await browser.new_page()
            await visit_link(page, product_url, wait_until="networkidle")

            if not (
                document := await parse_document(await page.content(), engine="lexbor")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                ) from err
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data from product url ({product_url}) even after 10 retries"
        )

    await page.close()

    crawl_data = MonostereoCrawlData(
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        category=category_name,
        product_name=product_name,
        model_name=model_name,
        option3=option3,
        message3=message3,
        quantity=quantity,
        price2=price2,
        price3=price3,
        option2=option2,
        model_name2=model_name2,
        period=period,
        message4=message4,
        manufacturer=manufacturer,
        percent=percent,
        brand=brand,
        option4=option4,
        message1=message1,
        detailed_images_html_source=detailed_images_html_source,
        message2=message2,
        delivery_fee=delivery_fee,
        option1=option1,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx,
        category_state.name.replace("<", r"\<"),
        category_state.pageno,
        crawl_data.product_url,
    )

    return None


async def is_bad_link(page: PlaywrightPage):
    if text := await page.text_content("body"):
        if (
            "An error has happened during application run. See exception log for details."
            in text
        ):
            return True

    if text := await page.text_content("body"):
        if (
            "The page you requested was not found, and we have a fine guess why."
            in text
        ):
            return True

    return False


async def extract_data(
    page: PlaywrightPage,
    document: Document,
    browser: PlaywrightBrowser,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_product_name(document),
        extract_model_name(document),
        extract_option3(document),
        extract_message3(document),
        extract_quantity(document),
        extract_price2(document),
        extract_price3(document),
        extract_option2(document),
        extract_table(document),
        extract_message2(document),
        extract_delivery_fee(document),
    )

    (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11) = await gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            await page.close()

            page = await browser.new_page()
            await visit_link(page, product_url, wait_until="networkidle")

            if not (
                document2 := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            document = document2

            tasks = (
                extract_product_name(document),
                extract_model_name(document),
                extract_option3(document),
                extract_message3(document),
                extract_quantity(document),
                extract_price2(document),
                extract_price3(document),
                extract_option2(document),
                extract_table(document),
                extract_message2(document),
                extract_delivery_fee(document),
            )
            (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11) = await gather(*tasks)  # type: ignore

            match R1:
                case Ok(product_name):
                    pass
                case Err(err):
                    raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R3:
        case Ok(option3):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R4:
        case Ok(message3):
            pass
        case Err(err):
            raise error.Message3NotFound(err, url=product_url)

    match R5:
        case Ok(quantity):
            pass
        case Err(err):
            raise error.QuantityNotFound(err, url=product_url)

    match R6:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R7:
        case Ok(price3):
            pass
        case Err(err):
            raise error.Price3NotFound(err, url=product_url)

    match R8:
        case Ok(option2):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    match R9:
        case Ok(table):
            (
                model_name2,
                period,
                message4,
                manufacturer,
                percent,
                brand,
                option4,
                message1,
            ) = table
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R10:
        case Ok(message2):
            pass
        case Err(err):
            raise error.Message2NotFound(err, url=product_url)

    match R11:
        case Ok(delivery_fee):
            pass
        case Err(err):
            raise error.DeliveryFeeNotFound(err, url=product_url)

    match await extract_thumbnail_image(page, product_url):
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match await extract_category(page):
        case Ok(category):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    match await extract_option1(page):
        case Ok(option1):
            pass
        case Err(err):
            raise error.OptionsNotFound(err, url=product_url)

    detailed_images_html_source = await extract_html(
        page, product_url, html_top, html_bottom
    )

    return (
        thumbnail_image_url,
        category,
        product_name,
        model_name,
        option3,
        message3,
        quantity,
        price2,
        price3,
        option2,
        model_name2,
        period,
        message4,
        manufacturer,
        percent,
        brand,
        option4,
        message1,
        detailed_images_html_source,
        message2,
        delivery_fee,
        option1,
    )


@returns_future(error.QueryNotFound)
async def extract_category(page: PlaywrightPage):
    query = "ul.items"

    await page.wait_for_selector(query, state="visible")

    if not (category := await page.text_content(query)):
        raise error.QueryNotFound("category not found", query=query)

    if "Home" in category:
        category = "Home> " + category.replace("Home", "").strip()

    return category


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "h1.page-title > span"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("product_name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = ".product > h3 > a > span"
    return (
        model_name.strip() if (model_name := await document.text_content(query)) else ""
    )


@returns_future(error.QueryNotFound)
async def extract_option3(document: Document):
    query = ".product-info-stock-sku > .stock > span"
    return option3.strip() if (option3 := await document.text_content(query)) else ""


@returns_future(error.QueryNotFound)
async def extract_message3(document: Document):
    query = "div[itemprop='sku']"
    return message3.strip() if (message3 := await document.text_content(query)) else ""


@returns_future(error.QueryNotFound)
async def extract_quantity(document: Document):
    query = "div.product.attribute.sku > div:nth-child(4)"
    return quantity.strip() if (quantity := await document.text_content(query)) else ""


# Abort requests for discogs
async def block_agg(route: Any):
    if route.request.resource_type != "document":
        await route.abort()
    else:
        await route.continue_()


@returns_future(error.QueryNotFound)
async def extract_option1(page: PlaywrightPage):
    tracklist_text = ""
    query_quantity = "div.product.attribute.sku > div:nth-child(4)"
    query_message3 = "div[itemprop='sku']"

    if (quantity := await page.text_content(query_quantity)) and (
        message3 := await page.text_content(query_message3)
    ):
        if quantity == "Vinyl":
            # ? We don't want negative SKU number because it gives different results when searched on discogs
            # ? See: https://b2b.monostereo1stop.com/heathen-victims-of-deception-180-gram-vinyl-import-2-lp-s-evp-8719262026599.html
            message3 = message3.strip().removeprefix("-")

            # ? Filling the message3 in serach box and then clicking on search button is slow, that's why directly passing message3 in url and then open that url
            discogs_url = f"https://www.discogs.com/ko/search/?q={message3}&type=all"
            try:
                await page.route("**/*", block_agg)
                await visit_link(page, discogs_url)

            except Exception as err:
                raise error.InvalidURL(err, discogs_url) from err

            query_first_result = (
                "#search_results > li:first-child .card-release-title a"
            )
            if not await page.query_selector(query_first_result):
                return "NOT PRESENT"

            first_result_url = await page.get_attribute(query_first_result, "href")
            first_result_url = urljoin(page.url, first_result_url)
            await visit_link(page, first_result_url)

            query_tracklist_text = "table[class*='tracklist'] tr"

            if not (tracklists := await page.query_selector_all(query_tracklist_text)):
                raise error.QueryNotFound(
                    f"Tracklist text not found at url: {first_result_url}",
                    query=query_tracklist_text,
                )

            for tracklist in tracklists:
                if trackPos := await tracklist.query_selector("td[class*='trackPos']"):
                    tracklist_text += f"{await trackPos.text_content()} "

                if artist := await tracklist.query_selector("td[class*='artist']"):
                    tracklist_text += f"{await artist.text_content()} "

                if trackTitle := await tracklist.query_selector(
                    "td[class*='trackTitle'] > span"
                ):
                    tracklist_text += f"{await trackTitle.text_content()} "

                if duration := await tracklist.query_selector("td[class*='duration']"):
                    tracklist_text += f"{await duration.text_content()} "

                tracklist_text = f"{tracklist_text.strip()}\n"

    return tracklist_text.strip()


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(document: Document):
    query = ".price-wrapper  > .price"
    if not (price2_str := await document.text_content(query)):
        raise error.QueryNotFound("price2 not found", query=query)

    try:
        price2 = extract_price_with_decimalpoint(price2_str)
    except ValueError as err:
        raise ValueError(f"Coudn't convert price2 text {price2_str} to number") from err

    return price2


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(document: Document):
    query = ".srlp-price > span"
    if not (price3_str := await document.text_content(query)):
        return ""

    try:
        price3 = extract_price_with_decimalpoint(price3_str)
    except ValueError as err:
        raise ValueError(f"Coudn't convert price3 text {price3_str} to number") from err

    return price3


@returns_future(error.QueryNotFound)
async def extract_option2(document: Document):
    query = ".tocart > span"
    return option2.strip() if (option2 := await document.text_content(query)) else ""


@returns_future(error.QueryNotFound, ValueError)
async def extract_table(document: Document):
    model_name2 = period = message4 = manufacturer = percent = brand = option4 = (
        message1
    ) = ""
    ths: list[Element] = []
    tds: list[Element] = []

    query = ".data.table.additional-attributes > tbody"
    if not (table_tbodys := await document.query_selector_all(query)):
        raise error.QueryNotFound("Table not found", query=query)

    for table_tbody in table_tbodys:
        ths.extend(await table_tbody.query_selector_all("tr > th"))
        tds.extend(await table_tbody.query_selector_all("tr > td"))

    for th, td in zip(ths, tds):
        if not (heading := await th.text_content()):
            continue

        if not (text := await td.text_content()):
            continue

        # model_name2
        if "Artist" in heading:
            model_name2 = text.strip()

        # period
        if "Format" in heading:
            period = text.strip()

        # message4
        if "Catalog" in heading:
            message4 = text.strip()

        # manufacturer
        if "Manufacturer" in heading:
            manufacturer = text.strip()

        # percent
        if "Released" in heading:
            percent = str(text)

        # brand
        if "Genre" in heading:
            brand = text.strip()

        # option4
        if "In Stock" in heading:
            option4 = text.strip()

        # message1
        if "Tracklist" in heading:
            message1 = text.strip()

    return (
        model_name2,
        period,
        message4,
        manufacturer,
        percent,
        brand,
        option4,
        message1,
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(page: PlaywrightPage, product_url: str):
    query = ".fotorama__active:nth-child(2) > img.fotorama__img"
    if not (thumbnail_image := await page.get_attribute(query, "src")):
        try:
            # https://b2b.monostereo1stop.com/kiss-kiss-rock-and-roll-over-fitted-jersey-tee-evp-844355069059.html
            thumbnail_image = await page.get_attribute(
                ".fotorama__active > img.fotorama__img", "src"
            )
        except:
            raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_message2(document: Document):
    query = ".product.attribute.description"
    return message2.strip() if (message2 := await document.text_content(query)) else ""


@returns_future(error.QueryNotFound)
async def extract_delivery_fee(document: Document):
    query = ".ma_fee_list span"
    if not (delivery_fee_str := await document.text_content(query)):
        return ""

    try:
        delivery_fee = extract_price_with_decimalpoint(delivery_fee_str)
    except ValueError as err:
        raise ValueError(
            f"Coudn't convert delivery_fee text {delivery_fee_str} to number"
        ) from err

    return delivery_fee


@cache
def text_quries():
    return ".product.attribute.description"


@returns_future(error.QueryNotFound)
async def extract_text(page: PlaywrightPage) -> str:
    query = text_quries()
    if not (html := await page.inner_html(query)):
        return "NOT PRESENT"

    # if text present on page
    elem = BeautifulSoup(html, features="lxml")
    text = get_text_from_html(elem)

    # remove extra new lines from text
    text = re.sub(r"\n+", "\n", text).strip()

    # replace \n with <br>
    return "<div>{}</div>".format(text.replace("\n", "<br>"))


async def extract_html(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    match await extract_text(page):
        case Ok(product_detail_text):
            return build_detailed_images_html(
                product_detail_text, html_top, html_bottom
            )

        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            return "NOT PRESENT"


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    await element.scroll_into_view_if_needed()
    await element.focus()


def build_detailed_images_html(text: str, html_top: str, html_bottom: str) -> str:
    """
    Build HTML from the texts based on our template

    """
    if text == "NOT PRESENT":
        return "NOT PRESENT"

    html = "".join(
        [
            html_top,
            text,
        ],
    )

    return html.strip()


def get_text_from_html(elem: Any):
    """
    It will extract text from html, preserve new line

    """
    text = ""
    for e in elem.descendants:
        if isinstance(e, str):
            text += e.strip()
        elif e.name in ["br", "div", "strong", "h1", "h2", "h3", "p"]:
            text += "\n"
    return text
