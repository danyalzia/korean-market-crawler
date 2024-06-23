# Author: Danyal Zia Khan
# Email: danyal6870@gmail.com
# Copyright (c) 2020-2024 Danyal Zia Khan
# All rights reserved.

from __future__ import annotations

import asyncio

from dataclasses import dataclass
from functools import cache
from typing import Any, cast
from urllib.parse import urljoin

import pandas as pd

from playwright.async_api import async_playwright

from dunia.browser import BrowserConfig
from dunia.document import Document
from dunia.element import Element
from dunia.error import HTMLParsingError
from dunia.extraction import fetch_content, load_content, parse_document, visit_link
from dunia.login import LoginInfo
from dunia.playwright import (
    AsyncPlaywrightBrowser,
    PlaywrightBrowser,
    PlaywrightElementHandle,
    PlaywrightPage,
)
from market_crawler import error, log
from market_crawler.apis import config
from market_crawler.apis.data import ApisCrawlData
from market_crawler.crawling import ConcurrentCrawler, crawl_categories
from market_crawler.excel import save_series_csv, to_series
from market_crawler.helpers import chunks, compile_regex, parse_int
from market_crawler.html import CategoryHTML
from market_crawler.initialization import Category, get_categories
from market_crawler.path import temporary_csv_file, temporary_custom_urls_csv_file
from market_crawler.settings import Settings
from market_crawler.state import CategoryState, get_category_state, get_product_state
from market_crawler.template import build_detailed_images_html
from robustify.error import MaxTriesReached
from robustify.functional import do, isin
from robustify.result import Err, Ok, Result, returns_future


async def login_button_strategy(page: PlaywrightPage, login_button_query: str):
    # ? Most markets redirect from login page when submitting the form, so we are putting it inside expect_navigation() block
    # ? If it doesn't work for some markets, then remove expect_navigation() and put the code outside it
    async with page.expect_navigation():
        await page.click(login_button_query)

    await page.wait_for_selector(
        'img[src="./images/top_menu_2016/top_logout_menu_01.jpg"]'
    )


def log_product_crawled_entries(
    idx: int, category: str, page_no: int, current_url: str, total_product_entries: int
):
    log.success(
        f"<magenta>No: {idx + 1}</><cyan> | C: {category}</><light-yellow> | Page: {page_no}</><blue> | {current_url}</> has been crawled <BLUE><w>({total_product_entries} product entries are present)</w></BLUE>"
    )


@cache
def next_page_url(*, current_url: str, next_page_no: int) -> str:
    if "?pg" in current_url:
        return current_url.replace(f"?pg={next_page_no-1}", f"?pg={next_page_no}")
    if "&pg" in current_url:
        return current_url.replace(f"&pg={next_page_no-1}", f"&pg={next_page_no}")

    return f"{current_url}&pg={next_page_no}"


async def run(settings: Settings) -> None:
    browser_config = BrowserConfig(
        headless=config.HEADLESS,
        default_navigation_timeout=config.DEFAULT_NAVIGATION_TIMEOUT,
        default_timeout=config.DEFAULT_TIMEOUT,
    )
    login_info = LoginInfo(
        login_url=config.LOGIN_URL,
        user_id=config.ID,
        password=config.PW,
        user_id_query="input[name='username']",
        password_query="input[name='password']",
        login_button_query='input[type=image][src="./images/sub/btn_login.gif"]',
    )
    async with async_playwright() as playwright:
        browser = await AsyncPlaywrightBrowser(
            browser_config=browser_config, playwright=playwright, login_info=login_info
        ).create()
        columns = list(settings.COLUMN_MAPPING.values())

        if not settings.URLS:
            categories = await get_categories(sitename=config.SITENAME, rate_limit=1)
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
    urls: list[str],
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
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
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        table,
        thumbnail_image_url,
        detailed_images_html_source,
    ) = await extract_data(document, page, product_url, html_top, html_bottom)

    if len(table.product_entries) > 1:
        for product_entry in table.product_entries:
            crawl_data = ApisCrawlData(
                category="CUSTOM",
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                manufacturing_country=table.manufacturing_country,
                manufacturer=table.manufacturer,
                detailed_images_html_source=detailed_images_html_source,
                delivery_fee=table.delivery_fee,
                model_name=product_entry.model_name,
                model_name2=product_entry.model_name2,
                percent=product_entry.percent,
                price2=product_entry.price2,
                price3=product_entry.price3,
                sold_out_text=product_entry.soldout_text,
                message1=product_entry.message1,
            )
            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log.action.product_custom_url_crawled_with_options(
            idx,
            product_url,
            len(table.product_entries),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    crawl_data = ApisCrawlData(
        category="CUSTOM",
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        manufacturing_country=table.manufacturing_country,
        manufacturer=table.manufacturer,
        detailed_images_html_source=detailed_images_html_source,
        delivery_fee=table.delivery_fee,
        model_name=table.product_entries[0].model_name,
        model_name2=table.product_entries[0].model_name2,
        percent=table.product_entries[0].percent,
        price2=table.product_entries[0].price2,
        price3=table.product_entries[0].price3,
        sold_out_text=table.product_entries[0].soldout_text,
        message1=table.product_entries[0].message1,
    )

    await save_series_csv(
        to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
    )

    log.action.product_custom_url_crawled(
        idx,
        product_url,
    )

    product_state.done = True
    if config.USE_PRODUCT_SAVE_STATES:
        await product_state.save()

    return None


async def crawl(
    category: Category,
    browser: PlaywrightBrowser,
    settings: Settings,
    columns: list[str],
):
    if not (
        category_state := await get_category_state(
            config=config,
            category_name=category.name,
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
    category_page_url = next_page_url(
        current_url=category.url, next_page_no=category_state.pageno
    )

    log.action.visit_category(category.name, category_page_url)

    while True:
        category_page_url = next_page_url(
            current_url=category_page_url, next_page_no=category_state.pageno
        )
        log.detail.page_url(category_page_url)

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
                    category_html,
                    filename,
                    columns,
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


@returns_future(error.QueryNotFound)
async def get_product_link(product: Element, category_url: str):
    if not (product_link := await product.query_selector("a")):
        raise error.QueryNotFound("Product link not found", "a")

    return urljoin(category_url, await product_link.get_attribute("href"))


async def visit_subcategory(page: PlaywrightPage):
    subcategories = await page.query_selector_all("#leftNav > div > ul > li")
    if total_subcategories := len(subcategories):
        for idx in range(total_subcategories):
            async with page.expect_navigation():
                await (await page.query_selector_all("#leftNav > div > ul > li"))[
                    idx
                ].click()

            yield cast(
                str,
                await (await page.query_selector_all("#leftNav > div > ul > li"))[
                    idx
                ].text_content(),
            ).strip(), page.url
    else:
        yield "", page.url


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
        content = await fetch_content(
            browser=browser,
            url=category_page_url,
            rate_limit=config.DEFAULT_RATE_LIMIT,
        )
        if not (document := await parse_document(content, engine="lexbor")):
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
    await visit_link(page, product_url, wait_until="load")

    if not (document := await parse_document(await page.content(), engine="lexbor")):
        raise HTMLParsingError("Document is not parsed correctly", url=product_url)

    (
        product_name,
        table,
        thumbnail_image_url,
        detailed_images_html_source,
    ) = await extract_data(document, page, product_url, html_top, html_bottom)

    if len(table.product_entries) > 1:
        for product_entry in table.product_entries:
            crawl_data = ApisCrawlData(
                category=category_state.name,
                product_url=product_url,
                product_name=product_name,
                thumbnail_image_url=thumbnail_image_url,
                manufacturing_country=table.manufacturing_country,
                manufacturer=table.manufacturer,
                detailed_images_html_source=detailed_images_html_source,
                delivery_fee=table.delivery_fee,
                model_name=product_entry.model_name,
                model_name2=product_entry.model_name2,
                percent=product_entry.percent,
                price2=product_entry.price2,
                price3=product_entry.price3,
                sold_out_text=product_entry.soldout_text,
                message1=product_entry.message1,
            )
            await save_series_csv(
                to_series(crawl_data, settings.COLUMN_MAPPING), columns, filename
            )

        log_product_crawled_entries(
            idx,
            category_state.name,
            category_state.pageno,
            product_url,
            len(table.product_entries),
        )
        product_state.done = True
        if config.USE_PRODUCT_SAVE_STATES:
            await product_state.save()

        return None

    crawl_data = ApisCrawlData(
        category=category_state.name,
        product_url=product_url,
        product_name=product_name,
        thumbnail_image_url=thumbnail_image_url,
        manufacturing_country=table.manufacturing_country,
        manufacturer=table.manufacturer,
        detailed_images_html_source=detailed_images_html_source,
        delivery_fee=table.delivery_fee,
        model_name=table.product_entries[0].model_name,
        model_name2=table.product_entries[0].model_name2,
        percent=table.product_entries[0].percent,
        price2=table.product_entries[0].price2,
        price3=table.product_entries[0].price3,
        sold_out_text=table.product_entries[0].soldout_text,
        message1=table.product_entries[0].message1,
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


async def extract_data(
    document: Document,
    page: PlaywrightPage,
    product_url: str,
    html_top: str,
    html_bottom: str,
):
    tasks = (
        extract_product_name(document),
        extract_table(page, product_url),
        extract_thumbnail_image(document, product_url),
    )

    R1, R2, R3 = await asyncio.gather(*tasks)

    match R1:
        case Ok(product_name):
            pass
        case Err(err):
            raise error.ProductNameNotFound(err, url=product_url)

    match R2:
        case Ok(table):
            pass
        case Err(err):
            raise error.TableNotFound(err, url=product_url)

    match R3:
        case Ok(thumbnail_image_url):
            pass
        case Err(err):
            raise error.ThumbnailNotFound(err, url=product_url)

    match await extract_images(page, product_url, html_top, html_bottom):
        case Ok(detailed_images_html_source):
            pass
        case Err(err):
            raise error.ProductDetailImageNotFound(err, url=product_url)

    return product_name, table, thumbnail_image_url, detailed_images_html_source


def get_productid(url: str) -> Result[str, str]:
    regex = compile_regex(r"ai_id=(\d+)")
    return (
        Ok(str(match.group(1)))
        if (match := regex.search(url))
        else Err(f"Regex Not Matched: {url}")
    )


async def has_products(document: Document):
    match await get_products(document):
        case Ok(products):
            return len(products)
        case _:
            return None


@returns_future(error.QueryNotFound)
async def get_products(document: Document):
    query = "body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table:nth-child(3) > tbody > tr > td > table > tbody > tr:nth-child(1) > td > table > tbody > tr:nth-child(1) > td"
    if not (products := await document.query_selector_all(query)):
        raise error.QueryNotFound("Products not found", query)
    return products


@returns_future(error.QueryNotFound)
async def extract_product_name(document: Document):
    query = "body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table > tbody > tr > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(1) > td > table > tbody > tr > td"
    if not (product_name := await document.text_content(query)):
        raise error.QueryNotFound("Product name not found", query)

    return product_name.strip()


@returns_future(error.QueryNotFound)
async def extract_thumbnail_image(document: Document, product_url: str):
    query = "body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table > tbody > tr > td > table > tbody > tr > td:nth-child(1) > table > tbody > tr:nth-child(1) > td > img"
    if not (thumbnail_image := await document.get_attribute(query, "src")):
        raise error.QueryNotFound("Thumbnail image not found", query)

    return urljoin(product_url, thumbnail_image)


def extract_price_fromstring(string: str):
    regex = compile_regex(r"[|]?\s+?(\d+[,]?\d+)원")
    if match := regex.search(string):
        return parse_int(match.group(1))


def extract_delivery_fee_fromstring(string: str):
    regex = compile_regex(r"(\d+[,]?\d+)원")
    if match := regex.search(string):
        return parse_int(match.group(1))


@dataclass(slots=True, frozen=True)
class ProductEntry:
    model_name: str
    model_name2: str
    percent: str
    price2: int
    price3: int
    soldout_text: str
    message1: str


@dataclass(slots=True, frozen=True)
class Table:
    manufacturer: str
    manufacturing_country: str
    delivery_fee: int
    product_entries: list[ProductEntry]


async def extract_table(page: PlaywrightPage, product_url: str):
    manufacturing_country = manufacturer = ""
    delivery_fee = 0

    manufacturer = cast(
        str,
        await page.text_content(
            'body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table > tbody > tr > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td:right-of(:text("제조사"))'
        ),
    ).strip()
    # log.info(f"Manufacturer: {manufacturer}")
    manufacturing_country = cast(
        str,
        await page.text_content(
            'body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table > tbody > tr > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td:right-of(:text("원산지"))'
        ),
    ).strip()
    # log.info(f"Manufacturing country: {manufacturing_country}")
    delivery_fee_text = cast(
        str,
        await page.text_content(
            'body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table > tbody > tr > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td:right-of(:text("배송료"))'
        ),
    ).strip()
    assert "원" in delivery_fee_text

    # print(f"{delivery_fee_text = }")
    if not (delivery_fee := extract_delivery_fee_fromstring(delivery_fee_text)):
        raise error.DeliveryFeeNotFound(
            f"Delivery fee was not found in text {delivery_fee_text}\n{page.url}"
        )
    # log.info(f"Delivery fee: {delivery_fee}")

    await page.wait_for_load_state("networkidle", timeout=300000)
    await page.mouse.wheel(delta_x=0, delta_y=1000)

    if not (
        table_child_element := await page.query_selector(
            "body > table:nth-child(2) > tbody > tr:nth-child(5) > td > table > tbody > tr > td > table > tbody > tr > td:nth-child(3) > table > tbody > tr:nth-child(8) > td > table"
        )
    ):
        return Err(Exception("Table not found"))

    if not (table := await table_child_element.query_selector("xpath=..")):
        return Err(Exception("Table not found"))

    html = (await table.inner_html()).strip()

    try:
        df = pd.read_html(html)[0]
    except Exception as ex:
        return Err(ex)

    if all(x in df.columns for x in [0, 1, 2, 3, 4, 5, 6]):
        df.columns = df.iloc[0, :]  # type: ignore
        df = df.drop(0).reset_index(drop=True)  # type: ignore

    try:
        assert all(
            x in df.columns
            for x in [
                "선택",
                "품명",
                "상품코드",
                "할인율",
                "판매가격",
                "도매가격",
                "수량",
            ]
        )
    except AssertionError:
        try:
            assert all(
                x in df.columns
                for x in [
                    "선택",
                    "품명",
                    "상품코드",
                    "할인율",
                    "판매가격",
                    "세일가격",
                    "수량",
                ]
            )
        except AssertionError:
            return Err(
                AssertionError(
                    "Neither ['선택', \"품명\", \"상품코드\", \"할인율\", \"판매가격\", \"도매가격\", \"수량\"] nor ['선택', '품명', '상품코드', '할인율', '판매가격', '세일가격', '수량'] columns are not present in dataframe"
                )
            )

    message1_indexes: list[int] = [
        i for i in range(len(df)) if df.iloc[i].str.startswith("추가").all()  # type: ignore
    ]

    message_mapping: dict[int, str] = {}
    if message1_indexes:
        for message1_index in message1_indexes:
            message_mapping[message1_index] = str(df.iloc[message1_index][0])  # type: ignore
            log.warning(
                f"Message1 found: {message_mapping[message1_index]}\n{product_url}"
            )
        # ? We will now remove this row so that parse_int below isn't called on NaN or blank ('') string
        df = df.drop(message1_indexes, axis=0).reset_index(drop=True)  # type: ignore

    # df["판매가격"] = df["판매가격"].apply(lambda s: parse_int(s) if "원" in s else s)  # type: ignore
    df["판매가격"] = df["판매가격"].apply(lambda s: parse_int(s))  # type: ignore
    try:
        df["도매가격"] = df["도매가격"].apply(lambda s: parse_int(s))  # type: ignore
    except Exception:
        df["세일가격"] = df["세일가격"].apply(lambda s: parse_int(s))  # type: ignore

    try:
        dictionary: Any = df.reset_index(drop=True).to_dict()
    except Exception as ex:
        return Err(ex)

    product_entries: list[ProductEntry] = []

    for i in range(len(df)):  # type: ignore
        model_name = str(dictionary["품명"][i])
        model_name2 = str(dictionary["상품코드"][i])
        percent = dictionary["할인율"][i]
        try:
            price2 = dictionary["도매가격"][i]
        except Exception:
            price2 = dictionary["세일가격"][i]
        price3 = dictionary["판매가격"][i]
        soldout = dictionary["선택"][i]

        sold_out_text = (
            "품절" if "품절입고" in str(soldout) or "품절" in str(soldout) else ""
        )
        if message1_indexes and (i + 1) in message1_indexes:
            product_entries.extend(
                ProductEntry(
                    model_name,
                    model_name2,
                    percent,
                    price2,
                    price3,
                    sold_out_text,
                    message_mapping[message1_index],
                )
                for message1_index in message1_indexes
                if message1_index == (i + 1)
            )
        else:
            product_entries.append(
                ProductEntry(
                    model_name,
                    model_name2,
                    percent,
                    price2,
                    price3,
                    sold_out_text,
                    "",
                )
            )

    return Ok(Table(manufacturer, manufacturing_country, delivery_fee, product_entries))


@cache
def image_quries():
    return "body > table:nth-child(2) > tbody > tr:nth-child(8) > td > table > tbody td img"


async def focus_element(page: PlaywrightPage, element: PlaywrightElementHandle):
    await page.mouse.wheel(delta_x=0, delta_y=500)
    await page.mouse.wheel(delta_x=0, delta_y=-500)

    await element.scroll_into_view_if_needed()
    await element.focus()


@returns_future(error.QueryNotFound, error.Base64Present)
async def extract_images(
    page: PlaywrightPage, product_url: str, html_top: str, html_bottom: str
) -> str:
    query = image_quries()
    if elements := await page.query_selector_all(query):
        await page.click(query)
    else:
        raise error.QueryNotFound(
            "Product detail images are not present at all", query=query
        )

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

    return build_detailed_images_html(
        map(lambda url: urljoin(product_url, url), urls),
        html_top,
        html_bottom,
    )


async def get_srcs(el: PlaywrightElementHandle):
    return src if (src := await el.get_attribute("src")) else ""
