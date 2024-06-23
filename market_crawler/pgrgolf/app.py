# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from contextlib import suppress
from functools import cache
from typing import NamedTuple, overload
from urllib.parse import urljoin

import pandas as pd

from playwright.async_api import async_playwright

from dunia.aio import gather
from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import load_content, parse_document, visit_link
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.pgrgolf import config
from market_crawler.pgrgolf.data import PGRGolfCrawlData
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns, returns_future


@cache
def page_url(*, current_url: str, next_page_no: int) -> str:
    if "?page" in current_url:
        return current_url.replace(f"?page={next_page_no-1}", f"?page={next_page_no}")
    if "&page" in current_url:
        return current_url.replace(f"&page={next_page_no-1}", f"&page={next_page_no}")

    return f"{current_url}?page={next_page_no}"


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    await page.click(login_button_query)
    await page.wait_for_selector(login_button_query, state="detached")


async def run(settings: Settings) -> None:
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )

    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config,
            playwright=playwright,
        ).create()

        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            categories = await get_categories(sitename=config.SITENAME)

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
    product_url: str,
    series: list[dict[str, str | int]],
):
    html_top, html_bottom = (
        settings.DETAILED_IMAGES_HTML_SOURCE_TOP,
        settings.DETAILED_IMAGES_HTML_SOURCE_BOTTOM,
    )
    page = await browser.new_page()
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        category_text,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
        model_name,
        quantity,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    # ? If it exceeds 32767 characters limit of Excel, don't crawl the product
    if len(detailed_images_html_source) > 32767:
        log.warning(
            f"Skipping the product ({product_url}) because detail images HTML exceeds 32767 characters limit of Excel"
        )
        return None

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok((option1, price2, option2, option3)):
                    pass
                case Err(err):
                    raise error.OptionsNotFound(err, url=product_url)

            crawl_data = PGRGolfCrawlData(
                category=category_text,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                product_name=product_name,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                price3=price3,
                price2=price2,
                model_name=model_name,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
            )

            series.append(to_series(crawl_data, settings.COLUMN_MAPPING))

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(options),
        )
        return None

    crawl_data = PGRGolfCrawlData(
        category=category_text,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        price3=price3,
        price2=price2,
        model_name=model_name,
        quantity=quantity,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
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

        if not (products_len := await has_products(document)):
            log.action.products_not_present_on_page(
                category_page_url, category_state.pageno
            )
            break

        log.detail.total_products_on_page(products_len, category_state.pageno)

        products_chunk = chunks(range(products_len), config.MAX_PRODUCTS_CHUNK_SIZE)

        filename: str = temporary_csv_file(
            sitename=config.SITENAME,
            date=settings.DATE,
            category_name=category.name,
            page_no=category_state.pageno,
        )

        for chunk in products_chunk:
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
    await visit_link(page, product_url, wait_until="networkidle")

    if not (document := await parse_document(await page.content(), engine="lxml")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        category_text,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
        model_name,
        quantity,
        options,
        detailed_images_html_source,
    ) = await extract_data(page, document, product_url, html_top, html_bottom)

    await page.close()

    # ? If it exceeds 32767 characters limit of Excel, don't crawl the product
    if len(detailed_images_html_source) > 32767:
        log.warning(
            f"Skipping the product ({product_url}) because detail images HTML exceeds 32767 characters limit of Excel"
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    if options:
        for option1 in options:
            match split_options_text(option1, price2):
                case Ok((option1, price2, option2, option3)):
                    pass
                case Err(err):
                    raise error.OptionsNotFound(err, url=product_url)

            crawl_data = PGRGolfCrawlData(
                category=category_text,
                product_url=product_url,
                thumbnail_image_url=thumbnail_image_url,
                thumbnail_image_url2=thumbnail_image_url2,
                thumbnail_image_url3=thumbnail_image_url3,
                thumbnail_image_url4=thumbnail_image_url4,
                thumbnail_image_url5=thumbnail_image_url5,
                product_name=product_name,
                manufacturer=manufacturer,
                manufacturing_country=manufacturing_country,
                price3=price3,
                price2=price2,
                model_name=model_name,
                quantity=quantity,
                detailed_images_html_source=detailed_images_html_source,
                option1=option1,
                option2=option2,
                option3=str(option3),
            )

            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_crawled_with_options(
            idx,
            category_state.name,
            category_state.pageno,
            product_url,
            len(options),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()
        return None

    crawl_data = PGRGolfCrawlData(
        category=category_text,
        product_url=product_url,
        thumbnail_image_url=thumbnail_image_url,
        thumbnail_image_url2=thumbnail_image_url2,
        thumbnail_image_url3=thumbnail_image_url3,
        thumbnail_image_url4=thumbnail_image_url4,
        thumbnail_image_url5=thumbnail_image_url5,
        product_name=product_name,
        manufacturer=manufacturer,
        manufacturing_country=manufacturing_country,
        price3=price3,
        price2=price2,
        model_name=model_name,
        quantity=quantity,
        detailed_images_html_source=detailed_images_html_source,
        option1="",
        option2="",
        option3="",
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_crawled(
        idx, crawl_data.category, category_state.pageno, crawl_data.product_url
    )

    return None


class Data(NamedTuple):
    category_text: str
    thumbnail_image_url: str
    thumbnail_image_url2: str
    thumbnail_image_url3: str
    thumbnail_image_url4: str
    thumbnail_image_url5: str
    product_name: str
    manufacturer: str
    manufacturing_country: str
    price3: int
    price2: int
    model_name: str
    quantity: str
    options: list[str]
    detailed_images_html_source: str


async def extract_data(
    page: PlaywrightPage,
    document: Document,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_category_text(document),
        extract_thumbnail_images(document, product_url),
        extract_product_name(document),
        extract_manufacturer(document),
        extract_manufacturing_country(document),
        extract_price3(document),
        extract_price2(document),
        extract_model_name(document),
        extract_quantity(document),
        extract_options(page),
    )

    (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10) = await gather(*tasks)

    match R1:
        case Ok(category_text):
            pass
        case Err(err):
            raise error.CategoryTextNotFound(err, url=product_url)

    match R2:
        case Ok(thumbnail_images):
            (
                thumbnail_image_url,
                thumbnail_image_url2,
                thumbnail_image_url3,
                thumbnail_image_url4,
                thumbnail_image_url5,
            ) = thumbnail_images
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match R3:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R4:
        case Ok(manufacturer):
            pass
        case Err(err):
            raise error.ManufacturerNotFound(err, url=product_url)

    match R5:
        case Ok(manufacturing_country):
            pass
        case Err(err):
            raise error.ManufacturerNotFound(err, url=product_url)

    match R6:
        case Ok(price3):
            pass
        case Err(err):
            raise error.Price3NotFound(err, url=product_url)

    match R7:
        case Ok(price2):
            pass
        case Err(err):
            raise error.Price2NotFound(err, url=product_url)

    match R8:
        case Ok(model_name):
            pass
        case Err(err):
            raise error.ModelNameNotFound(err, url=product_url)

    match R9:
        case Ok(quantity):
            pass
        case Err(err):
            raise error.QuantityNotFound(err, url=product_url)

    match R10:
        case Ok(options):
            pass
        case Err(err):
            raise IndexError(f"{err} -> {product_url}")

    detailed_images_html_source = await extract_html(
        page, product_url, html_top, html_bottom
    )

    return Data(
        category_text,
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
        product_name,
        manufacturer,
        manufacturing_country,
        price3,
        price2,
        model_name,
        quantity,
        options,
        detailed_images_html_source,
    )


@cache
def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"\/(\d*)\/category\/(\d*)")
    return (
        Ok(f"{str(match.group(1))}_{str(match.group(2))}")
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document) -> int | None:
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@overload
async def get_products(
    document_or_page: Document,
) -> Result[list[Element], error.QueryNotFound]: ...


@overload
async def get_products(
    document_or_page: PlaywrightPage,
) -> Result[list[PlaywrightElementHandle], error.QueryNotFound]: ...


async def get_products(document_or_page: Document | PlaywrightPage):
    query = 'li[id^="anchorBoxId"]'
    return (
        Ok(products)
        if (products := await document_or_page.query_selector_all(query))
        else Err(error.QueryNotFound("Products not found", query))
    )


@returns_future(error.ProductLinkNotFound)
async def get_product_link(product: Element, category_page_url: str):
    if not (product_link := await product.query_selector("div > a")):
        raise error.QueryNotFound("Product link not found", "div > a")

    return urljoin(category_page_url, await product_link.get_attribute("href"))


@returns_future(error.QueryNotFound)
async def extract_category_text(document: Document) -> str:
    query = "div.xans-element-.xans-product.xans-product-headcategory.path > ol > li:not([class='displaynone']) a"
    if not (category_texts := await document.query_selector_all(query)):
        raise error.QueryNotFound("Category text not found", query)

    return ">".join(
        [text.strip() for el in category_texts if (text := await el.text_content())]
    )


@returns_future(error.QueryNotFound)
async def extract_thumbnail_images(document: Document, product_url: str):
    query = "img.BigImage"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    thumbnail_image_url = urljoin(product_url, thumbnail_image)

    query = "img.ThumbImage"
    thumbnail_image_url2 = ""
    thumbnail_image_url3 = ""
    thumbnail_image_url4 = ""
    thumbnail_image_url5 = ""

    if thumbnail_images := (await document.query_selector_all(query))[1:]:
        thumbnail_images = [
            urljoin(product_url, await image.get_attribute("src"))
            for image in thumbnail_images
        ]
        N = 4
        thumbnail_images += [""] * (N - len(thumbnail_images))
        (
            thumbnail_image_url2,
            thumbnail_image_url3,
            thumbnail_image_url4,
            thumbnail_image_url5,
            *_,
        ) = thumbnail_images

    return (
        thumbnail_image_url,
        thumbnail_image_url2,
        thumbnail_image_url3,
        thumbnail_image_url4,
        thumbnail_image_url5,
    )


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "xpath=//tr[contains(./th, '상품명')]/td/span"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query=query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturer(document: Document):
    query = "xpath=//tr[contains(./th, '제조사')]/td/span"
    if not (manufacturer := await document.text_content(query)):
        raise error.QueryNotFound("Manufacturer not found", query=query)

    return manufacturer.strip()


@returns_future(error.QueryNotFound)
async def extract_manufacturing_country(document: Document):
    query = "xpath=//tr[contains(./th, '원산지')]/td/span"
    if not (manufacturing_country := await document.text_content(query)):
        raise error.QueryNotFound("Manufacturing country not found", query=query)

    return manufacturing_country.strip()


@returns_future(error.QueryNotFound)
async def extract_price3(document: Document):
    query = "xpath=//tr[contains(./th, '소비자가')]/td/span"
    if not (price3 := await document.text_content(query)):
        raise error.QueryNotFound("Price3 country not found", query=query)

    price3 = parse_int(price3.strip())

    return price3


@returns_future(error.QueryNotFound)
async def extract_price2(document: Document):
    query = "xpath=//tr[contains(./th, '판매가')]/td/span"
    if not (price2 := await document.text_content(query)):
        raise error.QueryNotFound("Price2 country not found", query=query)

    price2 = parse_int(price2.strip())

    return price2


@returns_future(error.QueryNotFound)
async def extract_model_name(document: Document):
    query = "xpath=//tr[contains(./th, '상품코드')]/td/span"
    if not (model_name := await document.text_content(query)):
        raise error.QueryNotFound("Model name not found", query=query)

    return model_name.strip()


@returns_future(error.QueryNotFound)
async def extract_quantity(document: Document):
    query = "div p.info"
    if not (quantity := await document.text_content(query)):
        raise error.QueryNotFound("Quantity not found", query)

    quantity = quantity.split("/")[0].strip() + ")"
    return quantity


@returns_future(IndexError)
async def extract_options(page: PlaywrightPage):
    with suppress(error.PlaywrightTimeoutError):
        await page.wait_for_load_state("networkidle")

    options: list[str] = []

    option1_query = "select[id='product_option_id1']"
    option2_query = "select[id='product_option_id2']"

    option1_elements = await page.query_selector_all(
        f"{option1_query} > option, {option1_query} > optgroup > option"
    )

    option1 = {
        "".join(text.split()): value
        for option1 in option1_elements
        if (text := await option1.text_content())
        and (value := await option1.get_attribute("value"))
        and value not in ["", "*", "**"]
    }

    for option1_text, option1_value in option1.items():
        # ? When there are a lot of requests at once, select_option() throws TimeoutError, so let's backoff here
        try:
            await page.select_option(
                option1_query,
                value=option1_value,
            )
        except error.PlaywrightTimeoutError as err:
            await page.reload()
            raise error.TimeoutException(
                "Timed out waiting for select_option()"
            ) from err

        try:
            await page.wait_for_load_state("networkidle")
        except error.PlaywrightTimeoutError as err:
            await page.reload()
            raise error.TimeoutException(
                "Timed out waiting for wait_for_load_state()"
            ) from err

        if option2_elements := await page.query_selector_all(
            f"{option2_query} > option, {option2_query} > optgroup > option"
        ):
            option2 = {
                "".join(text.split()): value
                for option2 in option2_elements
                if (text := await option2.text_content())
                and (value := await option2.get_attribute("value"))
                and value not in ["", "*", "**"]
            }

            options.extend(f"{option1_text},{option2_text}" for option2_text in option2)
        else:
            options.append(option1_text)

    return options


@returns(IndexError, ValueError)
def split_options_text(option1: str, price2: int):
    option3 = 0

    if "(+" in option1:
        regex = compile_regex(r"\s?\(\+￦?\w+[,]?\w*\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 += additional_price
            option3 += additional_price

    if "(-" in option1:
        regex = compile_regex(r"\s?\(\-￦?\w+[,]?\w*\)")
        for additional_price in regex.findall(option1):
            option1 = regex.sub("", option1)
            additional_price = parse_int(additional_price)
            price2 -= additional_price
            option3 = f"-{additional_price}"

    if "[품절]" in option1:
        return option1.replace("[품절]", ""), price2, "[품절]", option3 or ""
    if "품절" in option1:
        return option1.replace("품절", ""), price2, "품절", option3 or ""

    return option1, price2, "", option3 or ""


@cache
def image_quries():
    return "#prdDetail img"


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> list[str]:
    query = image_quries()

    elements = await page.query_selector_all(query)

    urls: list[str] = []
    for el in elements:
        action = lambda: get_srcs(el)
        focus = lambda: focus_element(page, el)
        is_base64 = isin("base64")
        match await do(action).retryif(
            predicate=is_base64,
            on_retry=focus,
            max_tries=5,
        ):
            case Ok(src):
                urls.append(src)
            case Err(MaxTriesReached(err)):
                raise error.Base64Present(err)

    if not urls:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

    return list(map(lambda url: urljoin(product_url, url), urls))


async def extract_html(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(images):
            images = list(dict.fromkeys(images))
            return build_detailed_images_html(
                images,
                html_top,
                html_bottom,
            )
        case Err(error.QueryNotFound(err)):
            log.debug(f"{err} -> {product_url}")
            return "NOT PRESENT"
        case Err(err):
            raise error.ProductDetailImageNotFound(err, product_url)


async def get_srcs(el: PlaywrightElementHandle):
    return (
        src
        if (src := await el.get_attribute("src"))
        and not await el.get_attribute("data-fr-image-pasted")
        else ""
    )


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.evaluate("() => { window.scrollBy(0, window.innerHeight); }")

    with suppress(error.PlaywrightTimeoutError):
        await element.click(timeout=1000)
        await page.wait_for_timeout(1000)
