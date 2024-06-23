# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from functools import cache
from typing import NamedTuple
from urllib.parse import urljoin

from aiofile import AIOFile
from playwright.async_api import Route, async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.playwright import AsyncPlaywrightBrowser, PlaywrightBrowser
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.memory import MemoryOptimizer
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.vinyltap import config
from market_crawler.vinyltap.data import VinyltapCrawlData
from robustify.result import Err, Ok, Result, returns_future


@cache
def extract_price_with_decimalpoint(text: str) -> str:
    # ? In VINYLTAP, most prices contain the floating point values
    try:
        return "".join(compile_regex(r"(\d*\.?\d+|\d+)").findall(text))
    except ValueError as e:
        raise ValueError(f"Text don't have any digit: '{text}'") from e


# ? Abort specific type of requests for page speedup
async def block_requests(route: Route):
    if any(
        url in route.request.url
        for url in {
            "paypal.com",
            "analytics.js",
            "prompt.js",
            "mixins.js",
            "interior.jpg",
            "exterior.jpg",
            "client-analytics",
            "chrome-extension",
            "compat.js",
            "payments.braintree-api.com",
        }
    ) or route.request.resource_type in [
        "preflight",
        "ping",
        "image",
        "font",
        "script",
        "stylesheet",
        "other",
    ]:
        await route.abort()
    else:
        await route.continue_()


async def find_categories(browser: PlaywrightBrowser):
    full_categories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "https://www.vinyltap.co.uk/", wait_until="networkidle")

    # ? Genre section
    await page.click(
        "#mainmenu > ul > li.nav-item.level0.nav-1.level-top.first.last.nav-item--parent.mega.nav-item--only-subcategories.parent > a"
    )

    categories = await page.query_selector_all(
        "#mainmenu > ul > li.nav-item.level0.nav-1.level-top.first.last.nav-item--parent.mega.nav-item--only-subcategories.parent.open > div > div > div > ul > li.nav-item.level1 > a"
    )

    for category in categories:
        if not (el := await category.query_selector("span")):
            continue

        if not (text := await el.text_content()):
            continue

        text = text.strip()

        if not (url := await category.get_attribute("href")):
            continue

        full_categories.append(Category(text, url))

    await page.close()

    async with AIOFile("categories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format("\n".join(f"{cat.name}, {cat.url}" for cat in full_categories))
        )

    return full_categories


@cache
def page_url(current_url: str, next_page_no: int) -> str:
    if "?p" in current_url:
        return current_url.replace(f"?p={next_page_no-1}", f"?p={next_page_no}")
    if "&p" in current_url:
        return current_url.replace(f"&p={next_page_no-1}", f"&p={next_page_no}")

    return f"{current_url}?p={next_page_no}"


async def run(settings: Settings) -> None:
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
        viewport={"width": 1280, "height": 720},
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
        ).create()

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            if await asyncio.to_thread(os.path.exists, "categories.txt"):
                categories = await get_categories(
                    sitename=config.SITENAME, filename="categories.txt"
                )
            else:
                categories = await find_categories(browser)

            log.detail.total_categories(len(categories))

            crawler = ConcurrentCrawler(
                categories=categories,
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
    await page.route("**/*", block_requests)

    for retry in range(1, 11):
        try:
            await visit_link(page, product_url, wait_until="load")

            if not (
                document := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            (
                product_name,
                model_name,
                message3,
                message2,
                message4,
                price2,
                category_text,
                thumbnail_image_url,
                option1,
                model_name2,
                percent,
                manufacturing_country,
                quantity,
                period,
                manufacturer,
                price1,
                message1,
                detailed_images_html_source,
            ) = await extract_data(document, product_url)
        except (
            error.PlaywrightError,
            error.QueryNotFound,
            error.CategoryTextNotFound,
            error.ThumbnailNotFound,
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

            await asyncio.sleep(retry)
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data from product url ({product_url}) even after 10 retries"
        )

    await page.close()

    crawl_data = VinyltapCrawlData(
        product_url=product_url,
        product_name=product_name,
        model_name=model_name,
        message3=message3,
        message2=message2,
        message4=message4,
        price2=price2,
        category=category_text,
        thumbnail_image_url=thumbnail_image_url,
        option1=option1,
        model_name2=model_name2,
        percent=percent,
        manufacturing_country=manufacturing_country,
        quantity=quantity,
        period=period,
        manufacturer=manufacturer,
        price1=price1,
        message1=message1,
        detailed_images_html_source=detailed_images_html_source,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_custom_url_crawled(
        idx,
        product_url,
    )

    return None


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    category_page_url = category.url
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
        name=category.name,
        pageno=category_state.pageno,
        date=settings.DATE,
        sitename=config.SITENAME,
    )
    log.action.visit_category(category_name, category_page_url)

    memory_optimizer = MemoryOptimizer(
        max_products_chunk_size=config.MAX_PRODUCTS_CHUNK_SIZE,
    )
    category_chunk_size = config.CATEGORIES_CHUNK_SIZE
    products_chunk_size = config.MIN_PRODUCTS_CHUNK_SIZE

    while True:
        category_page_url = page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
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
        log.detail.page_url(category_page_url)

        if not (document := await parse_document(content, engine="lxml")):
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

        log.action.category_page_crawled(category_name, category_state.pageno)

        category_state.pageno += 1
        category_html.pageno = category_state.pageno

        if config.USE_CATEGORY_SAVE_STATES:
            await category_state.save()

    category_state.done = True
    if config.USE_CATEGORY_SAVE_STATES:
        await category_state.save()


@cache
def get_productid(url: str) -> Result[str, str]:
    return (
        Ok(split[-1]) if (split := url.split("/")) else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document) -> int | None:
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "#maincontent > div.columns > div.column.main > div.products.wrapper.grid.items-grid.items-grid-partitioned.category-products-grid.hover-effect.equal-height > ol > li > div > div.product-item-img"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


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
    if not (document := await parse_document(content, engine="lxml")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    if not (product_url := (await get_product_link(product, category_page_url)).ok()):
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
                raise error.ProductsNotFound(err, url=category_page_url)

    productid = get_productid(product_url).expect(
        f"Product ID not found: {product_url}"
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
    await page.route("**/*", block_requests)

    for retry in range(1, 11):
        try:
            await visit_link(page, product_url, wait_until="load")

            if not (
                document := await parse_document(await page.content(), engine="lxml")
            ):
                raise HTMLParsingError(
                    "Document is not parsed correctly", url=product_url
                )

            (
                product_name,
                model_name,
                message3,
                message2,
                message4,
                price2,
                category_text,
                thumbnail_image_url,
                option1,
                model_name2,
                percent,
                manufacturing_country,
                quantity,
                period,
                manufacturer,
                price1,
                message1,
                detailed_images_html_source,
            ) = await extract_data(document, product_url)
        except (
            error.PlaywrightError,
            error.QueryNotFound,
            error.CategoryTextNotFound,
            error.ThumbnailNotFound,
            error.ProductNameNotFound,
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

            await asyncio.sleep(retry)
        else:
            break
    else:
        raise AssertionError(
            f"Couldn't extract the data from product url ({product_url}) even after 10 retries"
        )

    await page.close()

    crawl_data = VinyltapCrawlData(
        product_url=product_url,
        product_name=product_name,
        model_name=model_name,
        message3=message3,
        message2=message2,
        message4=message4,
        price2=price2,
        category=category_text,
        thumbnail_image_url=thumbnail_image_url,
        option1=option1,
        model_name2=model_name2,
        percent=percent,
        manufacturing_country=manufacturing_country,
        quantity=quantity,
        period=period,
        manufacturer=manufacturer,
        price1=price1,
        message1=message1,
        detailed_images_html_source=detailed_images_html_source,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx, category_state.name, category_state.pageno, crawl_data.product_url
    )

    return None


class Data(NamedTuple):
    product_name: str
    model_name: str
    message3: str
    message2: str
    message4: str
    price2: str
    category_text: str
    thumbnail_image_url: str
    option1: str
    model_name2: str
    percent: str
    manufacturing_country: str
    quantity: str
    period: str
    manufacturer: str
    price1: str
    message1: str
    detailed_images_html_source: str


async def extract_data(document: Document, product_url: str):
    tasks = (
        extract_product_name(document),
        extract_model_name(document),
        extract_message3(document),
        extract_message2(document),
        extract_message4(document),
        extract_price2(document),
        extract_category_text(document),
        extract_thumbnail_image(document, product_url),
        extract_option1(document),
        extract_model_name2(document),
        extract_percent(document),
        extract_manufacturing_country(document),
        extract_quantity(document),
        extract_period(document),
        extract_manufacturer(document),
        extract_price1(document),
        extract_message1(document),
        extract_images(document),
    )

    (
        R1,
        R2,
        R3,
        R4,
        R5,
        R6,
        R7,
        R8,
        R9,
        R10,
        R11,
        R12,
        R13,
        R14,
        R15,
        R16,
        R17,
        R18,
    ) = await gather(*tasks)

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
        case Ok(message3):
            pass
        case Err(err):
            # ? Some pages don't have message3
            # ? See: https://www.vinyltap.co.uk/ph0805772630529-sixtyblack-to-the-futurenotes-of-life-3cd
            log.warning(f"Messag3 not found: {product_url}")
            message3 = ""

    match R4:
        case Ok(message2):
            pass
        case Err(err):
            # ? Some pages don't have message2
            # ? See: https://www.vinyltap.co.uk/ca0349223001716-return-to-the-37th-chamber
            log.warning(f"Message2 not found: {product_url}")
            message2 = ""

    match R5:
        case Ok(message4):
            pass
        case Err(err):
            # ? Some pages don't have message2
            # ? See: https://www.vinyltap.co.uk/pr093074025222-pioneering-women-of-bluegrass-the-definitive-edition
            log.warning(f"Message4 not found: {product_url}")
            message4 = ""

    match R6:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R7:
        case Ok(category_text):
            pass
        case Err(err):
            # ? Some pages have strange category text which is equivalent to product name
            # ? See: https://www.vinyltap.co.uk/ii-0761331
            log.warning(f"Category text not found: {product_url}")
            category_text = product_name

    match R8:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R9:
        case Ok(option1):
            pass
        case Err(err):
            # ? Some pages don't have option1
            # ? See: https://www.vinyltap.co.uk/ph0805772630529-sixtyblack-to-the-futurenotes-of-life-3cd
            log.warning(f"Option1 not found: {product_url}")
            option1 = ""

    match R10:
        case Ok(model_name2):
            pass
        case Err(err):
            # ? Some pages don't have model_name2
            # ? See: https://www.vinyltap.co.uk/black-seeds-of-vengeance-0757640
            log.warning(f"Model name 2 not found: {product_url}")
            model_name2 = ""

    match R11:
        case Ok(percent):
            pass
        case Err(err):
            # ? Some pages don't have percent
            # ? See: https://www.vinyltap.co.uk/drama-box-0138704
            log.warning(f"Percent not found: {product_url}")
            percent = ""

    match R12:
        case Ok(manufacturing_country):
            pass
        case Err(err):
            # ? Some pages don't have manufacturing country
            # ? See: https://www.vinyltap.co.uk/pi5400863082802-epacr
            log.warning(f"Manufacturing country not found: {product_url}")
            manufacturing_country = ""

    match R13:
        case Ok(quantity):
            pass
        case Err(err):
            # ? Some pages don't have quantity
            # ? See: https://www.vinyltap.co.uk/antibalas-10th-anniversary-edition-0770193
            log.warning(f"Quantity not found: {product_url}")
            quantity = ""

    match R14:
        case Ok(period):
            pass
        case Err(err):
            # ? Some pages don't have period
            # ? See: https://www.vinyltap.co.uk/here-without-you-0142123
            log.warning(f"Period not found: {product_url}")
            period = ""

    match R15:
        case Ok(manufacturer):
            pass
        case Err(err):
            # ? Some pages don't have manufacturer
            # ? See: https://www.vinyltap.co.uk/la0194399867716-dirt
            log.warning(f"Manufacturer not found: {product_url}")
            manufacturer = ""

    match R16:
        case Ok(price1):
            pass
        case Err(err):
            # ? Some pages don't have price1
            # ? See: https://www.vinyltap.co.uk/jellies-060931
            log.warning(f"Price1 not found: {product_url}")
            price1 = ""

    match R17:
        case Ok(message1):
            pass
        case Err(err):
            # ? Some pages don't have track list
            # ? See: https://www.vinyltap.co.uk/what-can-i-do-0666670
            log.warning(f"Message1 (Track list) not found: {product_url}")
            message1 = ""

    match R18:
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            # ? Some pages don't have detail iamges (text)
            # ? See: https://www.vinyltap.co.uk/expensive-shit-he-miss-road-0772256
            log.warning(f"Product detail images not found: {product_url}")
            detailed_images_html_source = ""

    return Data(
        product_name,
        model_name,
        message3,
        message2,
        message4,
        price2,
        category_text,
        thumbnail_image_url,
        option1,
        model_name2,
        percent,
        manufacturing_country,
        quantity,
        period,
        manufacturer,
        price1,
        message1,
        detailed_images_html_source,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "#maincontent > div.columns > div > div.product-view.product-columns-wrapper > div.product-primary-column.product-shop.grid12-5.product-info-main > div.page-title-wrapper.product > h1 > span"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "#maincontent > div.columns > div > div.product-view.product-columns-wrapper > div.product-primary-column.product-shop.grid12-5.product-info-main > div.page-sub-title-wrapper.product > h4 > span"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query)

    return model_name.strip()


@returns_future(error.QueryNotFound)
async def extract_message3(document: Document):
    query = "#maincontent > div.columns > div > div.product-view.product-columns-wrapper > div.product-primary-column.product-shop.grid12-5.product-info-main > div.product-info-main > div.product.attribute.overview > div"
    if not (message3 := await document.text_content(query)):
        raise error.QueryNotFound("Message3 not found", query)

    return message3.strip()


@returns_future(error.QueryNotFound)
async def extract_message2(document: Document):
    query = "#maincontent > div.columns > div > div.product-view.product-columns-wrapper > div.product-primary-column.product-shop.grid12-5.product-info-main > div.product-info-main > div.product-info-price > div.product-info-stock-sku > div.stock.available > span"
    if not (message2 := await document.text_content(query)):
        raise error.QueryNotFound("Message2 not found", query)

    return message2.strip()


@returns_future(error.QueryNotFound)
async def extract_message4(document: Document):
    query = "#maincontent > div.columns > div > div.product-view.product-columns-wrapper > div.product-primary-column.product-shop.grid12-5.product-info-main > div.product-info-main > div.product-info-price > div.product-info-stock-sku > div.product.attribute.sku > div"
    if not (message2 := await document.text_content(query)):
        raise error.QueryNotFound("Message4 not found", query)

    return message2.strip()


@returns_future(error.QueryNotFound)
async def extract_price2(document: Document):
    query = "span.price"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 not found", query)

    price2 = extract_price_with_decimalpoint(price2.strip())

    return price2


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document):
    query = "meta[property='og:title']"
    if not (category_text := await document.get_attribute(query, "content")):
        raise error.QueryNotFound("Category text not found", query)

    return f"Home>{category_text}"


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "meta[property='og:image']"
    if not (thumbnail_image := await document.get_attribute(query, "content")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_option1(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Music Styles')]/td"
    if not (option1 := await document.text_content(query)):
        raise error.QueryNotFound("Option1 not found", query)

    return option1.strip()


@returns_future(error.QueryNotFound)
async def extract_model_name2(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Artist')]/td"
    if not (model_name2 := await document.inner_text(query)):
        raise error.QueryNotFound("Model name 2 not found", query)

    return model_name2.strip().removesuffix("(").strip()


@returns_future(error.QueryNotFound)
async def extract_percent(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Grade')]/td"
    if not (percent := await document.text_content(query)):
        raise error.QueryNotFound("Percent not found", query)

    return percent.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturing_country(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Country Item Pressed In')]/td"
    if not (manufacturing_country := await document.text_content(query)):
        raise error.QueryNotFound("Manufacturing country not found", query)

    return manufacturing_country.strip()


@returns_future(error.QueryNotFound)
async def extract_quantity(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Format')]/td"
    if not (quantity := await document.text_content(query)):
        raise error.QueryNotFound("Quantity not found", query)

    return quantity.strip()


@returns_future(error.QueryNotFound)
async def extract_period(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Year')]/td"
    if not (period := await document.text_content(query)):
        raise error.QueryNotFound("Period not found", query)

    return period.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturer(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Label')]/td"
    if not (manufacturer := await document.inner_text(query)):
        raise error.QueryNotFound("Manufacturer not found", query)

    return manufacturer.strip().removesuffix("(").strip()


@returns_future(error.QueryNotFound)
async def extract_price1(document: Document):
    query = "xpath=//tr[contains(th/text(), 'Catalogue Number')]/td"
    if not (price1 := await document.text_content(query)):
        raise error.QueryNotFound("Price1 not found", query)

    # ? Even though it says price1, it is actually Catalogue Number, so we are not going to use parse_int()
    return price1.strip()


@returns_future(error.QueryNotFound)
async def extract_message1(document: Document):
    query = "#tracklist > div"
    if not (message1 := await document.text_content(query)):
        raise error.QueryNotFound("Message1 not found", query)

    message1 = message1.strip()

    return message1


@returns_future(error.QueryNotFound)
async def extract_images(document: Document) -> str:
    query = "#description > div"
    if not (images_text := await document.text_content(query)):
        raise error.QueryNotFound("Images text not found", query)

    images_text = images_text.strip()

    return images_text
