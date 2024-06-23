# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio
import os

from dataclasses import dataclass
from functools import cache
from typing import overload
from urllib.parse import urljoin

import pandas as pd

from aiofile import AIOFile
from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_page, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.purefishing import config
from market_crawler.purefishing.data import PurefishingCrawlData
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state
from market_crawler.template import build_detailed_images_html
from robustify.result import Err, Ok, Result, returns_future


@cache
def next_page_url(*, current_url: str, next_page_no: int) -> str:
    if "?pg" in current_url:
        return current_url.replace(f"?pg={next_page_no-1}", f"?pg={next_page_no}")
    if "&pg" in current_url:
        return current_url.replace(f"&pg={next_page_no-1}", f"&pg={next_page_no}")

    return f"{current_url}&pg={next_page_no}"


async def find_subcategories(browser: PlaywrightBrowser):
    full_subcategories: list[Category] = []

    page = await browser.new_page()
    await visit_link(page, "http://www.purefishing.co.kr/member")

    categories = await page.query_selector_all(
        "#sticky-wrapper > aside > div > ul > li"
    )

    for category in categories:
        if not (
            category_text := (
                await (await category.query_selector_all("button"))[0].text_content()
            )
        ):
            continue

        category_text = category_text.strip()

        subcategories = await category.query_selector_all("li a")
        for subcategory in subcategories:
            if not (subcategory_text := (await subcategory.text_content())):
                continue

            subcategory_text = subcategory_text.strip()
            url = urljoin(page.url, await subcategory.get_attribute("href"))
            full_subcategories.append(
                Category(f"{category_text}>{subcategory_text}", url)
            )

    await page.close()

    async with AIOFile("subcategories.txt", "w", encoding="utf-8-sig") as afp:
        await afp.write(
            "{}".format(
                "\n".join(f"{cat.name}, {cat.url}" for cat in full_subcategories)
            )
        )

    return full_subcategories


def get_login_info():
    return LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="#mId",
        password_query="#mPw",
        login_button_query="#main > section > div > div.field > button",
    )


async def run(settings: Settings) -> None:
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = get_login_info()
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            if os.path.exists("subcategories.txt"):
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
    series: list[dict[str, str | int]] = []
    for chunk in chunks(range(len(urls)), config.MAX_PRODUCTS_CHUNK_SIZE):
        tasks = (
            extract_url(
                idx,
                browser,
                settings,
                urls[idx],
                series,
            )
            for idx in chunk
        )

        await asyncio.gather(*tasks)

    filename: str = temporary_custom_urls_csv_file(
        sitename=config.SITENAME,
        date=settings.DATE,
    )
    await asyncio.to_thread(
        pd.DataFrame(series, columns=columns).to_csv,  # type: ignore
        filename,  # type: ignore
        encoding="utf-8-sig",
        index=False,
    )


async def extract_url(
    idx: int,
    browser: PlaywrightBrowser,
    settings: Settings,
    category_page_url: str,
    series: list[dict[str, str | int]],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, category_page_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    match await extract_table(product, page):
        case Ok(table):
            pass
        case Err(err):
            raise err

    number_of_products = len(products)

    # ? In case products handles have been changed after extract_table(), let's get them again
    match await get_products(page):
        case Ok(products):
            assert number_of_products == (
                current_number_of_products := len(products)
            ), f"Products length mismatch: {number_of_products} vs {current_number_of_products}"
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    if popup := await product.query_selector("a"):
        await popup.click()

    match await extract_thumbnail_image(page, category_page_url):
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            log.warning(
                f"Thumbnail image not found for product # {idx} <blue>({category_page_url})</>"
            )
            thumbnail_image_url = ""

    match await get_product_link(page, category_page_url):
        case Ok(product_url):
            pass
        case Err(err):
            raise error.ProductLinkNotFound(err, url=category_page_url)

    await visit_link(page, product_url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    await page.close()

    (R1, R2) = await asyncio.gather(
        extract_images(document, category_page_url, html_top, html_bottom),
        extract_message1(document),
    )

    match R1:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.warning(f"Detailed images not found: <blue>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    match R2:
        case Ok(message1):
            pass
        case Err(err):
            log.warning(f"Message1 not found: <blue>{product_url}</>")
            message1 = ""

    crawl_data = PurefishingCrawlData(
        product_url=product_url,
        product_name=table.product_name,
        thumbnail_image_url=thumbnail_image_url,
        option1=table.option1,
        model_name=table.model_name,
        price3=parse_int(table.prices.price3),
        price1=parse_int(table.prices.price1),
        price2=parse_int(table.prices.price2),
        quantity=table.quantity,
        sold_out_text=table.sold_out_text,
        detailed_images_html_source=detailed_images_html_source,
        message1=message1,
    )

    series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

    log.action.product_custom_url_crawled(idx, product_url)

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

    category_page_url = next_page_url(
        current_url=category_url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category_name, category_page_url)

    while True:
        category_page_url = next_page_url(
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

        if not (
            document := await parse_document(await page.content(), engine="modest")
        ):
            raise HTMLParsingError(
                "Document is not parsed correctly", url=category_page_url
            )
        await page.close()

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

        for chunk in chunks(range(number_of_products), config.MAX_PRODUCTS_CHUNK_SIZE):
            tasks = (
                extract_product(
                    idx,
                    browser,
                    category_page_url,
                    category_state,
                    filename,
                    columns,
                    number_of_products,
                    settings,
                )
                for idx in chunk
            )

            await asyncio.gather(*tasks)

        log.action.category_page_crawled(category_state.name, category_state.pageno)

        category_state.pageno += 1
        category_html.pageno = category_state.pageno

        if config.USE_CATEGORY_SAVE_STATES:
            await category_state.save()

    category_state.done = True
    if config.USE_CATEGORY_SAVE_STATES:
        await category_state.save()


async def extract_product(
    idx: int,
    browser: PlaywrightBrowser,
    category_page_url: str,
    category_state: CategoryState,
    filename: str,
    columns: list[str],
    number_of_products: int,
    settings: Settings,
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, category_page_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError(
            "Document is not parsed correctly", url=category_page_url
        )

    match await get_products(document):
        case Ok(products):
            assert number_of_products == (
                current_number_of_products := len(products)
            ), f"Products length mismatch: {number_of_products} vs {current_number_of_products}"
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    match await extract_table(product, page):
        case Ok(table):
            pass
        case Err(err):
            raise err

    # ? In case products handles have been changed after extract_table(), let's get them again
    match await get_products(page):
        case Ok(products):
            assert number_of_products == (
                current_number_of_products := len(products)
            ), f"Products length mismatch: {number_of_products} vs {current_number_of_products}"
            product = products[idx]
        case Err(err):
            raise error.ProductsNotFound(err, url=category_page_url)

    if popup := await product.query_selector("a"):
        await popup.click()

    match await extract_thumbnail_image(page, category_page_url):
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            log.warning(
                f"Thumbnail image not found for product # {idx} <blue>({category_page_url})</>"
            )
            thumbnail_image_url = ""

    match await get_product_link(page, category_page_url):
        case Ok(product_url):
            pass
        case Err(err):
            raise error.ProductLinkNotFound(err, url=category_page_url)

    await page.close()

    page = await browser.new_page()
    await visit_link(page, product_url)

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    await page.close()

    (R1, R2) = await asyncio.gather(
        extract_images(document, category_page_url, html_top, html_bottom),
        extract_message1(document),
    )

    match R1:
        case Ok(detailed_images_html_source):
            pass
        case Err(error.QueryNotFound(err)):
            log.warning(f"Detailed images not found: <blue>{product_url}</>")
            detailed_images_html_source = "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)

    match R2:
        case Ok(message1):
            pass
        case Err(err):
            log.warning(f"Message1 not found: <blue>{product_url}</>")
            message1 = ""

    crawl_data = PurefishingCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=table.product_name,
        thumbnail_image_url=thumbnail_image_url,
        option1=table.option1,
        model_name=table.model_name,
        price3=parse_int(table.prices.price3),
        price1=parse_int(table.prices.price1),
        price2=parse_int(table.prices.price2),
        quantity=table.quantity,
        sold_out_text=table.sold_out_text,
        detailed_images_html_source=detailed_images_html_source,
        message1=message1,
    )

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )

    return None


@dataclass(slots=True, frozen=True)
class Prices:
    price3: int
    price1: int
    price2: int


@dataclass(slots=True, frozen=True)
class Table:
    product_name: str
    option1: str
    model_name: str
    prices: Prices
    quantity: str
    sold_out_text: str


@returns_future(IndexError)
async def extract_table(product: Element, page: PlaywrightPage):
    headings = [
        text
        for heading in await page.query_selector_all(
            "#sticky-wrapper > div > div[class='th']"
        )
        if (text := await heading.text_content())
    ]

    tasks = (
        extract_product_name(headings, product),
        extract_option1(headings, product),
        extract_model_name(headings, product),
        extract_price3(headings, product),
        extract_price1(headings, product),
        extract_price2(headings, product),
        extract_quantity(headings, product),
        extract_soldout(headings, product),
    )

    (
        product_name,
        option1,
        model_name,
        price3,
        price1,
        price2,
        quantity,
        sold_out_text,
    ) = await asyncio.gather(*tasks)

    if any(
        isinstance(err := value.err(), ValueError)
        for value in (
            product_name,
            option1,
            model_name,
            price3,
            price1,
            price2,
            quantity,
            sold_out_text,
        )
    ):
        raise err

    return Table(
        product_name.unwrap(),
        option1.unwrap(),
        model_name.unwrap(),
        Prices(price3.unwrap(), price1.unwrap(), price2.unwrap()),
        quantity.unwrap(),
        sold_out_text.unwrap(),
    )


async def has_products(tree: Document) -> int | None:
    match await get_products(tree):
        case Ok(products):
            return len(products)
        case _:
            return None


@overload
async def get_products(
    tree: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    tree: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(
    tree: Document | PlaywrightPage,
):
    query = "div.product_list > div > div[class^='tr']"
    return (
        Ok(products)
        if (products := await tree.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(page: PlaywrightPage, category_page_url: str):
    try:
        product_selector = (
            await page.query_selector_all("#product_popup > div.popup_btn > a")
        )[0]
    except IndexError as err:
        raise error.QueryNotFound("Product link not found", "a") from err

    return urljoin(category_page_url, await product_selector.get_attribute("href"))


@returns_future(error.QueryNotFound, ValueError)
async def extract_product_name(headings: list[str], product: Element):
    try:
        idx = headings.index("모델명")
    except ValueError as err:
        raise ValueError(f'"모델명" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1}) > a"
    if not (product_name := await product.query_selector(query)):
        raise error.QueryNotFound("Product name not found", query)

    if text := await product_name.text_content():
        return text.strip()

    raise ValueError("Product name is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_option1(headings: list[str], product: Element):
    try:
        idx = headings.index("제품명")
    except ValueError as err:
        raise ValueError(f'"제품명" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (option1 := await product.query_selector(query)):
        raise error.QueryNotFound("Option1 not found", query)

    if text := await option1.text_content():
        return text.strip()

    raise ValueError("Option1 is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_model_name(headings: list[str], product: Element):
    try:
        idx = headings.index("제품번호")
    except ValueError as err:
        raise ValueError(f'"제품번호" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (modelname := await product.query_selector(query)):
        raise error.QueryNotFound("Model name not found", query)

    if text := await modelname.text_content():
        return text.strip()

    raise ValueError("Model name is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_price3(headings: list[str], product: Element):
    try:
        idx = headings.index("희망소비자가")
    except ValueError as err:
        raise ValueError(f'"희망소비자가" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (price3 := await product.query_selector(query)):
        raise error.QueryNotFound("Price3 not found", query)

    if text := await price3.text_content():
        return text.strip()

    raise ValueError("Price3 is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_price1(headings: list[str], product: Element):
    try:
        idx = headings.index("도매가")
    except ValueError as err:
        raise ValueError(f'"도매가" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (price1 := await product.query_selector(query)):
        raise error.QueryNotFound("Price1 not found", query)

    if text := await price1.text_content():
        return text.strip()

    raise ValueError("Price1 is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_price2(headings: list[str], product: Element):
    try:
        idx = headings.index("실출고가")
    except ValueError as err:
        raise ValueError(f'"실출고가" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (price2 := await product.query_selector(query)):
        raise error.QueryNotFound("Price2 not found", query)

    if text := await price2.text_content():
        return text.strip()

    raise ValueError("Price2 is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_quantity(headings: list[str], product: Element):
    try:
        idx = headings.index("포장수량")
    except ValueError as err:
        raise ValueError(f'"포장수량" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (quantity := await product.query_selector(query)):
        raise error.QueryNotFound("Quantity not found", query)

    if text := await quantity.text_content():
        return text.strip()

    raise ValueError("Quantity is empty")


@returns_future(error.QueryNotFound, ValueError)
async def extract_soldout(headings: list[str], product: Element):
    try:
        idx = headings.index("주문(포장)수량")
    except ValueError as err:
        raise ValueError(f'"주문(포장)수량" is not present in {headings}') from err

    # ? We are doing +1 because nth-child index starts at 1 and not 0
    query = f"div:nth-child({idx + 1})"
    if not (soldout := await product.query_selector(query)):
        raise error.QueryNotFound("Soldout not found", query)

    if text := await soldout.text_content():
        return text.strip()

    raise ValueError("Soldout text is empty")


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(page: PlaywrightPage, category_page_url: str):
    query = "#product_popup > div.popup_img > img"
    if not (thumbnail_image := await page.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(category_page_url, thumbnail_image)


@returns_future(error.QueryNotFound)
async def extract_message1(document: Document) -> str:
    query = "#main > section > div > div > section > table.info.info1 > tbody > tr > td"
    if not (text := await document.text_content(query)):
        raise error.QueryNotFound("Message1 not found", query=query)

    return text


@cache
def image_quries():
    return "#main > section > div > div > section > div.body img"


@returns_future(error.QueryNotFound, error.InvalidImageURL)
async def extract_images(
    document: Document, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()

    urls = [
        src
        for image in await document.query_selector_all(query)
        if (src := await image.get_attribute("src"))
    ]

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    if any("base64" in url for url in urls):
        raise error.InvalidImageURL("Base64 is present in images")

    return build_detailed_images_html(
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )
